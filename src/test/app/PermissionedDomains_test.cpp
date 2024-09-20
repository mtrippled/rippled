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
#include <iostream>
#include <optional>
#include <string>
#include <utility>
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

    static Json::Value
    deleteTx(AccountID const& account, std::string const& domain)
    {
        Json::Value jv{Json::objectValue};
        jv[sfTransactionType.jsonName] = jss::PermissionedDomainDelete;
        jv[sfAccount.jsonName] = to_string(account);
        jv[sfDomainID.jsonName] = domain;
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
        auto const setFee {drops(env.current()->fees().increment)};
        env(setTx(alice, tfSetOnlyXRP), fee(setFee), ter(tesSUCCESS));
        std::cerr << "test set finished\n";
        env.close();

        Json::Value params;
        params[jss::account] = alice.human();
        auto const resp = env.rpc("json", "account_objects", to_string(params));
        BEAST_EXPECT(resp["result"]["account_objects"].size() == 1);
        std::cerr << "account_objects:\n" << resp << '\n';
        Json::Value a{Json::arrayValue};
        a = resp[jss::result][jss::account_objects][0u][jss::index].asString();
        std::cerr << "array: " << a << '\n';
        std::cerr << "keylet.key: " << keylet::permissionedDomain(alice, 4).key << '\n';

        uint256 index;
        std::ignore = index.parseHex(a.asString());
        std::cerr << "uint256 index:" << index << '\n';
        env(deleteTx(alice, a.asString()), ter(tesSUCCESS));
        env.close();
        auto const resp2 = env.rpc("json", "account_objects", to_string(params));
        std::cerr << "account_objects:\n" << resp2 << '\n';
        BEAST_EXPECT(resp2["result"]["account_objects"].size() == 0);

//        BEAST_EXPECT(str)

//        BEAST_EXPECT(
//            resp[jss::result][jss::error_message] == "Account malformed.");


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
