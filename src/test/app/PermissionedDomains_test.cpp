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

#include <test/jtx.h>
#include <xrpl/basics/Blob.h>
#include <xrpl/basics/Slice.h>
#include <xrpl/protocol/Feature.h>
#include <xrpl/protocol/Issue.h>
#include <xrpl/protocol/jss.h>
#include <array>
#include <iostream>
#include <optional>
#include <vector>

namespace ripple {
namespace test {
namespace jtx {

class PermissionedDomains_test : public beast::unit_test::suite
{
    FeatureBitset withFeature_ {
        supported_amendments() | featurePermissionedDomains};
    FeatureBitset withoutFeature_ {supported_amendments()};

    static Json::Value
    setTx(AccountID const& account, std::uint32_t const flags,
        std::optional<uint256> domain = std::nullopt,
        std::optional<std::vector<Blob>> credentials = std::nullopt,
        std::optional<std::vector<Issue>> tokens = std::nullopt)
    {
        Json::Value jv;
        jv[sfTransactionType.jsonName] = jss::PermissionedDomainSet;
        jv[sfAccount.jsonName] = to_string(account);
        jv[sfFlags.jsonName] = flags;
        if (credentials)
        {
            Json::Value a(Json::arrayValue);
            for (auto const& credential : *credentials)
                a.append(strHex(Slice{credential.data(), credential.size()}));
            jv[sfAcceptedCredentials.jsonName] = a;
        }
        if (tokens)
        {
            Json::Value a(Json::arrayValue);
            for (auto const& token : *tokens)
                a.append(to_json(token));
            jv[sfAcceptedTokens.jsonName] = a;
        }
        return jv;
    }

public:
    void
    testEnabled()
    {
        testcase("Enabled");
        Account const alice{"alice"};
        Env env{*this, withFeature_};
        env.fund(XRP(1000), alice);
        std::cerr << "test set starting\n";
        env(setTx(alice, tfSetOnlyXRP), ter(tesSUCCESS));
        std::cerr << "test set finished\n";
        env.close();

        /*
         *                 auto jrr = env.rpc(
                    "json", "account_objects", to_string(params))[jss::result];

         */
        BEAST_EXPECT(true);
    }


    /*
     *         using namespace test::jtx;
        static FeatureBitset const all{supported_amendments()};
        static FeatureBitset const fixNFTDir{fixNFTokenDirV1};

        static std::array<FeatureBitset, 7> const feats{
            all - fixNFTDir - fixNonFungibleTokensV1_2 - fixNFTokenRemint -
                fixNFTokenReserve - featureNFTokenMintOffer,
            all - disallowIncoming - fixNonFungibleTokensV1_2 -
                fixNFTokenRemint - fixNFTokenReserve - featureNFTokenMintOffer,
            all - fixNonFungibleTokensV1_2 - fixNFTokenRemint -
                fixNFTokenReserve - featureNFTokenMintOffer,
            all - fixNFTokenRemint - fixNFTokenReserve -
                featureNFTokenMintOffer,
            all - fixNFTokenReserve - featureNFTokenMintOffer,
            all - featureNFTokenMintOffer,
            all};

        if (BEAST_EXPECT(instance < feats.size()))
        {
            testWithFeats(feats[instance]);
        }
        BEAST_EXPECT(!last || instance == feats.size() - 1);

     * Env env{*this, features};

     * if (env.current()->rules().enabled(fixNFTokenRemint))
     */
    void
    run() override
    {
        /*
        testcase("Amendment is there.");
        Env env{
            *this,
            supported_amendments() | featurePermissionedDomains
        };
        BEAST_EXPECT(
            env.current()->rules().enabled(featurePermissionedDomains));
            */

        testEnabled();
    }
};

BEAST_DEFINE_TESTSUITE_PRIO(PermissionedDomains, app, ripple, 2);

} // jtx
} // test
} // ripple
