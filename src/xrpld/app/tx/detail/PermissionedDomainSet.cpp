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

    constexpr std::size_t PD_ARRAY_MAX = 10;
    if (ctx.tx.isFieldPresent(sfAcceptedCredentials))
    {
        auto const credentials =
            ctx.tx.getFieldArray(sfAcceptedCredentials);
        if (credentials.size() == 0 || credentials.size() > PD_ARRAY_MAX)
            return temMALFORMED;
        /*
        // TODO iterate and make sure each Issuer exists once credentials
        // interface is available.
        for (auto const& credential : *credentials.value())
        {
        }
         */
    }

    if (ctx.tx.isFieldPresent(sfAcceptedTokens))
    {
        auto const tokens = ctx.tx.getFieldArray(sfAcceptedTokens);
        if (tokens.size() == 0 || tokens.size() > PD_ARRAY_MAX)
            return temMALFORMED;

        for (auto const& token : tokens)
        {
            auto asset = token.at(~sfAsset);
            if (asset)
            {
                if (isXRP(asset->currency))
                    return temMALFORMED;
                continue;
            }
            /* TODO check if MPTIssue type is XRP when the time comes.
            auto mptAsset = token.at(~mptAsset)
            if (mptAsset)
            {
            }
             */
        }
    }

    // Checking with existing domainID requires preclaim()
    if (!ctx.tx.isFieldPresent(sfDomainID))
    {
        auto const ret = checkRules(ctx.tx,
            std::make_shared<SLE>(keylet::permissionedDomain(
                ctx.tx.getAccountID(sfAccount),
                ctx.tx.getFieldU32(sfSequence))));
        if (!isTesSuccess(ret))
            return ret;
    }

    return preflight2(ctx);
}

XRPAmount
PermissionedDomainSet::calculateBaseFee(ReadView const& view, STTx const& tx)
{
    // The fee required for PermissionedDomainSet is one owner reserve.
    return view.fees().increment;
}

TER
PermissionedDomainSet::preclaim(PreclaimContext const& ctx)
{
    auto domain = ctx.tx.at(~sfDomainID);
    if (!domain)
        return tesSUCCESS;

    // Check existing object.
    auto const sleDomain = ctx.view.read(
        {ltPERMISSIONED_DOMAIN, *domain});
    if (sleDomain->empty())
        return tecNO_ENTRY;
    auto const owner = sleDomain->getAccountID(sfOwner);
    auto account = ctx.tx.getAccountID(sfAccount);
    if (owner != account)
        return temINVALID_ACCOUNT_ID;

    if (auto const ret = checkRules(ctx.tx, sleDomain);
        !isTesSuccess(ret))
    {
        return ret;
    }

    return tesSUCCESS;
}

/** Attempt to create the Permissioned Domain. */
TER
PermissionedDomainSet::doApply()
{
    auto const ownerSle = view().peek(keylet::account(account_));
    if (!ownerSle)
        return tefINTERNAL;

    auto updateSle =
        [this](std::shared_ptr<STLedgerEntry> const& sle) {
            auto const flags = ctx_.tx.getFlags();
            if (flags)
            {
                sle->setFlag(flags & tfSetOnlyXRP);
                sle->setFlag(flags & tfClearOnlyXRP);
            }
            if (ctx_.tx.isFieldPresent(sfAcceptedCredentials))
            {
                auto credentials = ctx_.tx.getFieldArray(sfAcceptedCredentials);
                /* TODO sort by Issuer once the interface to credentials is available.
                credentials.sort([](STObject const& left, STObject const& right) {
                    return left.? < right.?;
                });
                 */
                if (credentials.empty())
                    sle->delField(sfAcceptedCredentials);
                else
                    sle->setFieldArray(sfAcceptedCredentials, credentials);
            }
            if (ctx_.tx.isFieldPresent(sfAcceptedTokens))
            {
                auto tokens = ctx_.tx.getFieldArray(sfAcceptedTokens);
                tokens.sort([](STObject const &left, STObject const &right)
                {
                    // TODO incorporate MPTissue when the time comes.
                    return left.at(sfAsset).account < right.at(sfAsset).account;
                });
                if (tokens.empty())
                    sle->delField(sfAcceptedTokens);
                else
                    sle->setFieldArray(sfAcceptedTokens, tokens);
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
        slePd->setFieldU16(sfSequence, ctx_.tx.getFieldU16(sfSequence));
        slePd->setFieldArray(sfAcceptedCredentials, STArray());
        auto tokens = ctx_.tx.getFieldArray(sfAcceptedTokens);
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

NotTEC
PermissionedDomainSet::checkRules(STTx const& tx,
    std::shared_ptr<STLedgerEntry const> const& sle)
{
    bool const sleOnlyXRP = sle->getFlags() & lsfOnlyXRP;
    auto const txFlags = tx.getFlags();
    if ((txFlags & tfSetOnlyXRP) && (txFlags & tfClearOnlyXRP))
        return temMALFORMED;
    bool onlyXRP = false;
    if (sleOnlyXRP)
    {
        if (txFlags & tfSetOnlyXRP)
            return temMALFORMED;
        onlyXRP = true;
    }
    else
    {
        if (txFlags & tfClearOnlyXRP)
            return temMALFORMED;
        // onlyXRP is still false
    }

    bool someCredentials = false;
    if (tx.isFieldPresent(sfAcceptedCredentials))
    {
        if (!tx.getFieldArray(sfAcceptedCredentials).empty())
            someCredentials = true;
    }

    bool someTokens = false;
    if (tx.isFieldPresent(sfAcceptedTokens))
    {
        if (!tx.getFieldArray(sfAcceptedTokens).empty())
            someTokens = true;
    }

    if (onlyXRP)
    {
        if (someTokens)
            return temMALFORMED;
    }
    else
    {
        if (!someCredentials && !someTokens)
            return temMALFORMED;
    }

    return tesSUCCESS;
}

} // ripple
