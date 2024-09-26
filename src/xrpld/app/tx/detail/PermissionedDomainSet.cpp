//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2024 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <xrpl/protocol/STObject.h>
#include <xrpl/protocol/TxFlags.h>
#include <xrpld/app/tx/detail/PermissionedDomainSet.h>
#include <xrpld/ledger/View.h>

#include <optional>

namespace ripple {

NotTEC
PermissionedDomainSet::preflight(PreflightContext const& ctx)
{
    std::cerr << "preflight1\n";
    if (!ctx.rules.enabled(featurePermissionedDomains))
    {
        std::cerr << "preflight2\n";
        return temDISABLED;
    }
    if (auto const ret = preflight1(ctx); !isTesSuccess(ret))
    {
        std::cerr << "preflight3\n";
        return ret;
    }

    if (!ctx.tx.isFieldPresent(sfAcceptedCredentials))
    {
        std::cerr << "preflight4\n";
        return temMALFORMED;
    }

    constexpr std::size_t PD_ARRAY_MAX = 10;
    auto const credentials =
        ctx.tx.getFieldArray(sfAcceptedCredentials);
    if (credentials.empty() || credentials.size() > PD_ARRAY_MAX)
    {
        std::cerr << "preflight5\n";
        return temMALFORMED;
    }

    std::cerr << "preflight6\n";
    return preflight2(ctx);
}

XRPAmount
PermissionedDomainSet::calculateBaseFee(ReadView const& view, STTx const& tx)
{
    // The fee required for PermissionedDomainSet is one owner reserve.
    if (tx.isFieldPresent(sfDomainID))
        return Transactor::calculateBaseFee(view, tx);
    return view.fees().increment;
}

TER
PermissionedDomainSet::preclaim(PreclaimContext const& ctx)
{
    std::cerr << "preclaim1\n";
    if (!ctx.view.read(keylet::account(ctx.tx.getAccountID(sfAccount))))
        return tefINTERNAL;

    auto const domain = ctx.tx.at(~sfDomainID);
    if (!domain)
        return tesSUCCESS;

    // Check existing object.
    std::cerr << "checkzero1\n";
    if (*domain == beast::zero)
    {
        std::cerr << "malformed4\n";
        return temMALFORMED;
    }
    std::cerr << "checkzero3\n";
    auto const sleDomain = ctx.view.read(
        {ltPERMISSIONED_DOMAIN, *domain});
    if (!sleDomain)
        return tecNO_ENTRY;
    auto const owner = sleDomain->getAccountID(sfOwner);
    auto account = ctx.tx.getAccountID(sfAccount);
    if (owner != account)
        return temINVALID_ACCOUNT_ID;

    return tesSUCCESS;
}

/** Attempt to create the Permissioned Domain. */
TER
PermissionedDomainSet::doApply()
{
    std::cerr << "PermissionedDomainSet::doApply1\n";
    auto const ownerSle = view().peek(keylet::account(account_));
    std::cerr << "PermissionedDomainSet::doApply3\n";

    // All checks have already been done.
    auto updateSle =
        [this](std::shared_ptr<STLedgerEntry> const& sle) {
            auto credentials = ctx_.tx.getFieldArray(sfAcceptedCredentials);
            if (credentials.empty() && sle->isFieldPresent(sfAcceptedCredentials))
                sle->delField(sfAcceptedCredentials);
            else
            {
                std::cerr << "set credentials1 len " << credentials.size() << '\n';
                credentials.sort([](STObject const& left, STObject const& right) -> bool {
                    return dynamic_cast<STObject const*>(&left)->getAccountID(sfIssuer) <
                        dynamic_cast<STObject const*>(&right)->getAccountID(sfIssuer);
                });
                sle->setFieldArray(sfAcceptedCredentials, credentials);
            }
    };

    if (auto domain = ctx_.tx.at(~sfDomainID))
    {
        // Modify existing permissioned domain.
        auto sleUpdate = view().peek(
            {ltPERMISSIONED_DOMAIN, *domain});
        std::cerr << "modify before update flags:" << sleUpdate ->getFlags() << '\n';
        updateSle(sleUpdate);
        view().update(sleUpdate);
    }
    else
    {
        std::cerr << "PermissionedDomainSet::doApply10\n";
        // Create new permissioned domain.
        Keylet const pdKeylet = keylet::permissionedDomain(account_,
            ctx_.tx.getFieldU32(sfSequence));
        std::cerr << "PermissionedDomainSet::doApply11\n";
        auto slePd = std::make_shared<SLE>(pdKeylet);
        std::cerr << "new sle has credentials:"
            << slePd->isFieldPresent(sfAcceptedCredentials);
        slePd->setAccountID(sfOwner, account_);
        std::cerr << "PermissionedDomainSet::doApply12\n";
        slePd->setFieldU32(sfSequence, ctx_.tx.getFieldU32(sfSequence));
        std::cerr << "PermissionedDomainSet::doApply13\n";
        updateSle(slePd);
        std::cerr << "after update sle has credentials:"
                  << slePd->isFieldPresent(sfAcceptedCredentials) << '\n';
        std::cerr << "PermissionedDomainSet::doApply20\n";
        view().insert(slePd);
        std::cerr << "PermissionedDomainSet::doApply21\n";
        auto const page = view().dirInsert(
            keylet::ownerDir(account_),
            pdKeylet,
            describeOwnerDir(account_));
        std::cerr << "PermissionedDomainSet::doApply22\n";
        if (!page)
            return tecDIR_FULL;
        std::cerr << "PermissionedDomainSet::doApply23\n";
        slePd->setFieldU64(sfOwnerNode, *page);
        std::cerr << "PermissionedDomainSet::doApply24\n";
        // If we succeeded, the new entry counts against the creator's reserve.
        adjustOwnerCount(view(), ownerSle, 1, ctx_.journal);
    }

    std::cerr << "PermissionedDomainSet::doApply1000\n";
    return tesSUCCESS;
}

} // ripple
