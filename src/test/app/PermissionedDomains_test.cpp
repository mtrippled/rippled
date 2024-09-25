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
#include <xrpld/app/tx/detail/ApplyContext.h>
#include <xrpld/ledger/ApplyViewImpl.h>
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
    setTx(AccountID const& account,
        std::vector<std::pair<AccountID, Blob>> credentials,
        std::optional<uint256> domain = std::nullopt)
    {
        Json::Value jv;
        jv[sfTransactionType.jsonName] = jss::PermissionedDomainSet;
        jv[sfAccount.jsonName] = to_string(account);
        if (domain)
            jv[sfDomainID.jsonName] = to_string(*domain);
        Json::Value a(Json::arrayValue);
        for (auto const& credential : credentials)
        {
            Json::Value obj(Json::objectValue);
            obj[sfIssuer.jsonName] = to_string(credential.first);
            obj[sfCredentialType.jsonName] = strHex(Slice{credential.second.data(),
                                                 credential.second.size()});
            Json::Value o2(Json::objectValue);
            o2[sfAcceptedCredential.jsonName] = obj;
            a.append(o2);
        }
        jv[sfAcceptedCredentials.jsonName] = a;
        std::cerr << "json test set tx:" << jv << '\n';
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

    void
    testEnabled()
    {
        testcase("Enabled");
        Account const alice{"alice"};
        Env env{*this, withFeature_};
        env.fund(XRP(1000), alice);
        auto const setFee {drops(env.current()->fees().increment)};
        std::vector<std::pair<AccountID, Blob>> credentials;
        credentials.emplace_back(alice, Blob());
        env(setTx(alice, credentials));
        env.close();

        Json::Value params;
        params[jss::account] = alice.human();
        auto const resp = env.rpc("json", "account_objects", to_string(params));
        BEAST_EXPECT(resp["result"]["account_objects"].size() == 1);
        Json::Value a{Json::arrayValue};
        a = resp[jss::result][jss::account_objects][0u][jss::index].asString();

        uint256 index;
        std::ignore = index.parseHex(a.asString());
        uint256 d{};
        env(deleteTx(alice, to_string(index)), ter(tesSUCCESS));
        env.close();
        BEAST_EXPECT(env.rpc("json", "account_objects",
            to_string(params))["result"]["account_objects"].size() == 0);
    }

    void
    testDisabled()
    {
        testcase("Disabled");
        Account const alice{"alice"};
        Env env{*this, withoutFeature_};
        env.fund(XRP(1000), alice);
        auto const setFee {drops(env.current()->fees().increment)};
        env(deleteTx(alice, to_string(uint256(75))), ter(temDISABLED));
        env.close();
    }

    void
    testSet()
    {
        testcase("Set");
        {
            // Create new (with only XRP flag and no other rules)
            Account const alice{"alice"};
            Env env{*this, withFeature_};
            env.fund(XRP(1000), alice);
            auto const setFee{drops(env.current()->fees().increment)};
            env.close();

            Json::Value params;
            params[jss::account] = alice.human();
            Json::Value const objs = env.rpc("json", "account_objects",
                to_string(params))[jss::result][jss::account_objects];
            std::cerr << "objs:" << objs << '\n';
            BEAST_EXPECT(objs.size() == 1);
            BEAST_EXPECT(objs[0u].get("AcceptedCredentials", Json::nullValue) == Json::nullValue);

            // Update
            uint256 domain;
            std::ignore = domain.parseHex(objs[0u][jss::index].asString());
            std::vector<std::pair<AccountID, Blob>> credentials;
            credentials.emplace_back(alice, Blob());
            credentials.emplace_back(alice, Blob());
            std::cerr << "UPDATE TEST TX\n";
            env.close();
            Json::Value const obj = env.rpc("json", "account_objects",
                to_string(params))[jss::result][jss::account_objects];
            std::cerr << "obj:" << obj << '\n';
            BEAST_EXPECT(obj.size() == 1);
            BEAST_EXPECT(obj[0u]["AcceptedCredentials"].size() == 2);

            // Update non-existing

            // Update doesn't belong to owner.
            Account const bob{"bob"};
            env.fund(XRP(1000), bob);
            env.close();

            // Create object with no flags.
            env(setTx(bob, credentials), fee(setFee));
            env.close();
            params[jss::account] = bob.human();
            Json::Value objects = env.rpc("json", "account_objects",
                to_string(params))[jss::result][jss::account_objects];
            BEAST_EXPECT(objects.size() == 1);
            BEAST_EXPECT(objects[0u]["Flags"].asUInt() == 0);

            // Update object with bad flags.
            std::ignore = domain.parseHex(objects[0u][jss::index].asString());

            // Update object good flag.
            env.close();
            objects = env.rpc("json", "account_objects",
                to_string(params))[jss::result][jss::account_objects];
        }
    }

    void
    testDelete()
    {
        testcase("Delete");
        Env env{*this, withFeature_};

        Account const alice{"alice"};
        env.fund(XRP(1000), alice);
        auto const setFee {drops(env.current()->fees().increment)};
        Json::Value params;
        params[jss::account] = alice.human();
        std::string const aliceIndex = env.rpc("json", "account_objects",
            to_string(params))[jss::result][jss::account_objects][0u][jss::index].asString();

        Account const bob{"bob"};
        env.fund(XRP(1000), bob);
        env.close();

        // Delete a domain that doesn't belong to the account.
        uint256 index;
        std::ignore = index.parseHex(aliceIndex);
        env(deleteTx(bob, to_string(index)), ter(temINVALID_ACCOUNT_ID));
        // Delete a non-existent domain.
        env(deleteTx(alice, to_string(uint256(75))), ter(tecNO_ENTRY));
        // Delete uint256 zero domain.
        env(deleteTx(alice, to_string(uint256())), ter(temMALFORMED));
        // Delete domain that belongs to user.
        env(deleteTx(alice, to_string(index)), ter(tesSUCCESS));
    }

public:
    void
    run() override
    {
        testEnabled();
        testDisabled();
        testSet();
        testDelete();
    }
};

BEAST_DEFINE_TESTSUITE_PRIO(PermissionedDomains, app, ripple, 2);

} // jtx
} // test
} // ripple
