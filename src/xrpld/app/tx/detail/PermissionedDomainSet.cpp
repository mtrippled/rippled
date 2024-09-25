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
        if (credentials.empty() || credentials.size() > PD_ARRAY_MAX)
        {
            std::cerr << "malformed1\n";
            return temMALFORMED;
        }
    }

    if (ctx.tx.isFieldPresent(sfAcceptedTokens))
    {
        auto const tokens = ctx.tx.getFieldArray(sfAcceptedTokens);
        if (tokens.empty() || tokens.size() > PD_ARRAY_MAX)
        {
            std::cerr << "malformed2\n";
            return temMALFORMED;
        }

        for (auto const& token : tokens)
        {
            auto asset = token.at(~sfAsset);
            if (asset)
            {
                if (isXRP(asset->currency))
                {
                    std::cerr << "malformed3\n";
                    return temMALFORMED;
                }
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
    if (tx.isFieldPresent(sfDomainID))
        return view.fees().base;
    return view.fees().increment;
}

TER
PermissionedDomainSet::preclaim(PreclaimContext const& ctx)
{
    /*
    if (ctx.tx.isFieldPresent(sfAcceptedCredentials))
    {
        // TODO will need a credential object with issuer and
        // CredentialType (Blob) fields when credentials
        // implemented.
        auto const credentials =
            ctx.tx.getFieldArray(sfAcceptedCredentials);
        // TODO iterate and make sure each Issuer exists once credentials
        // interface is available.
        for (auto const& credential : credentials)
        {
            if (!credential.isFieldPresent(sfIssuer) ||
                !credential.isFieldPresent(sfCredentialType))
            {
                return temMALFORMED;
            }
            auto const issuer = credential.getAccountID(sfIssuer);
            auto const sle = ctx.view.read(keylet::account(issuer));
            if (!sle)
                return temBAD_ISSUER;
        }
    }
     */

    auto domain = ctx.tx.at(~sfDomainID);
    if (!domain)
        return tesSUCCESS;

    // Check existing object.
    std::cerr << "checkzero1\n";
    if (*domain == beast::zero)
    {
        std::cerr << "checkzero2\n";
        return temMALFORMED;
    }
    std::cerr << "checkzero3\n";
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
    std::cerr << "PermissionedDomainSet::doApply1\n";
    auto const ownerSle = view().peek(keylet::account(account_));
    if (!ownerSle)
        return tefINTERNAL;
    std::cerr << "PermissionedDomainSet::doApply3\n";

    // All checks have already been done.
    auto updateSle =
        [this](std::shared_ptr<STLedgerEntry> const& sle) {
            auto const flags = ctx_.tx.getFlags();
            if (flags)
            {
                if (flags & tfSetOnlyXRP)
                    sle->setFlag(lsfOnlyXRP);
                else if (flags & tfClearOnlyXRP)
                    sle->clearFlag(lsfOnlyXRP);
            }
            if (ctx_.tx.isFieldPresent(sfAcceptedCredentials))
            {
                auto credentials = ctx_.tx.getFieldArray(sfAcceptedCredentials);
                if (credentials.empty() && sle->isFieldPresent(sfAcceptedCredentials))
                    sle->delField(sfAcceptedCredentials);
                else
                {
                    // TODO will need a credential object with issuer and
                    // CredentialType (Blob) fields when credentials
                    // implemented.
                    std::cerr << "set credentials1 len " << credentials.size() << '\n';
                    credentials.sort([](STObject const& left, STObject const& right) -> bool {
                        return dynamic_cast<STObject const*>(&left)->getAccountID(sfIssuer) <
                            dynamic_cast<STObject const*>(&right)->getAccountID(sfIssuer);
                    });
                    sle->setFieldArray(sfAcceptedCredentials, credentials);
                }
            }
            if (ctx_.tx.isFieldPresent(sfAcceptedTokens))
            {
                auto tokens = ctx_.tx.getFieldArray(sfAcceptedTokens);
                tokens.sort([](STObject const &left, STObject const &right)
                {
                    // TODO incorporate MPTissue when the time comes.
                    return left.at(sfAsset).account < right.at(sfAsset).account;
                });
                if (tokens.empty() && sle->isFieldPresent(sfAcceptedTokens))
                    sle->delField(sfAcceptedTokens);
                else
                {
                    std::cerr << "set tokens1\n";
                    sle->setFieldArray(sfAcceptedTokens, tokens);
                }
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
        std::cerr << "new sle has credentials,tokens:"
            << slePd->isFieldPresent(sfAcceptedCredentials) << ','
            << slePd->isFieldPresent(sfAcceptedTokens) << '\n';
        slePd->setAccountID(sfOwner, account_);
        std::cerr << "PermissionedDomainSet::doApply12\n";
        slePd->setFieldU32(sfSequence, ctx_.tx.getFieldU32(sfSequence));
        std::cerr << "PermissionedDomainSet::doApply13\n";
        updateSle(slePd);
        std::cerr << "after update sle has credentials,tokens,flags:"
                  << slePd->isFieldPresent(sfAcceptedCredentials) << ','
                  << slePd->isFieldPresent(sfAcceptedTokens) << ','
                  << slePd->getFlags() << '\n';
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

NotTEC
PermissionedDomainSet::checkRules(STTx const& tx,
    std::shared_ptr<STLedgerEntry const> const& sle)
{
    /*
     * wrong flags already checked
     * onlyXRP:
     * true: sle has flag, no tx flag to unset
     * true: sle has no flag, tx flag to set
     */
    bool const txFlagSet = tx.getFlags() & tfSetOnlyXRP;
    bool const txFlagClear = tx.getFlags() & tfClearOnlyXRP;
    if (txFlagSet && txFlagClear)
    {
        std::cerr << "malformed4\n";
        return temMALFORMED;
    }
    bool onlyXRP = false;
    std::cerr << "sle->getFlags():" << sle->getFlags() << '\n';
    if (sle->getFlags() & lsfOnlyXRP)
    {
        if (txFlagSet)
        {
            std::cerr << "malformed5\n";
            return temMALFORMED;
        }
        onlyXRP = true;
    }
    else
    {
        if (txFlagClear)
        {
            std::cerr << "malformed6\n";
            return temMALFORMED;
        }
        else if (txFlagSet)
        {
            onlyXRP = true;
        }
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
        {
            std::cerr << "malformed7\n";
            return temMALFORMED;
        }
    }
    else
    {
        if (!someCredentials && !someTokens)
        {
            std::cerr << "malformed8\n";
            return temMALFORMED;
        }
    }

    return tesSUCCESS;
}

} // ripple
