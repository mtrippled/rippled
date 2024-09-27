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
    if (!ctx.rules.enabled(featurePermissionedDomains))
        return temDISABLED;
    if (auto const ret = preflight1(ctx); !isTesSuccess(ret))
        return ret;

    if (!ctx.tx.isFieldPresent(sfAcceptedCredentials))
        return temMALFORMED;
    constexpr std::size_t PD_ARRAY_MAX = 10;
    auto const credentials =
        ctx.tx.getFieldArray(sfAcceptedCredentials);
    if (credentials.empty() || credentials.size() > PD_ARRAY_MAX)
        return temMALFORMED;

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
    if (!ctx.view.read(keylet::account(ctx.tx.getAccountID(sfAccount))))
        return tefINTERNAL;

    auto const credentials = ctx.tx.getFieldArray(sfAcceptedCredentials);
    for (auto const& credential : credentials)
    {
        if (!credential.isFieldPresent(sfIssuer) ||
            !credential.isFieldPresent(sfCredentialType))
        {
            return temMALFORMED;
        }
        auto const issuer = credential.getAccountID(sfIssuer);
        auto sle = ctx.view.read(keylet::account(issuer));
        if (!sle)
            return temBAD_ISSUER;
    }

    auto const domain = ctx.tx.at(~sfDomainID);
    if (!domain)
        return tesSUCCESS;

    // Check existing object.
    if (*domain == beast::zero)
        return temMALFORMED;
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
    auto const ownerSle = view().peek(keylet::account(account_));

    // All checks have already been done.
    auto updateSle =
        [this](std::shared_ptr<STLedgerEntry> const& sle) {
            auto credentials = ctx_.tx.getFieldArray(sfAcceptedCredentials);
            if (credentials.empty() && sle->isFieldPresent(sfAcceptedCredentials))
                sle->delField(sfAcceptedCredentials);
            else
            {
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
        updateSle(sleUpdate);
        view().update(sleUpdate);
    }
    else
    {
        // Create new permissioned domain.
        Keylet const pdKeylet = keylet::permissionedDomain(account_,
            ctx_.tx.getFieldU32(sfSequence));
        auto slePd = std::make_shared<SLE>(pdKeylet);
        slePd->setAccountID(sfOwner, account_);
        slePd->setFieldU32(sfSequence, ctx_.tx.getFieldU32(sfSequence));
        updateSle(slePd);
        view().insert(slePd);
        auto const page = view().dirInsert(
            keylet::ownerDir(account_),
            pdKeylet,
            describeOwnerDir(account_));
        if (!page)
            return tecDIR_FULL;
        slePd->setFieldU64(sfOwnerNode, *page);
        // If we succeeded, the new entry counts against the creator's reserve.
        adjustOwnerCount(view(), ownerSle, 1, ctx_.journal);
    }

    return tesSUCCESS;
}

} // ripple
