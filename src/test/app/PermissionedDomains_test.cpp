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
#include <algorithm>
#include <iostream>
#include <map>
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

    // helpers
    using Credential = std::pair<AccountID, Blob>;
    using Credentials = std::vector<Credential>;

    static Json::Value
    setTx(AccountID const& account,
        Credentials const& credentials,
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
    deleteTx(AccountID const& account, uint256 const& domain)
    {
        Json::Value jv{Json::objectValue};
        jv[sfTransactionType.jsonName] = jss::PermissionedDomainDelete;
        jv[sfAccount.jsonName] = to_string(account);
        jv[sfDomainID.jsonName] = to_string(domain);
        return jv;
    }

    static std::map<uint256, Json::Value>
    getObjects(Account const& account, Env& env)
    {
        std::cerr << "objects for " << account.human() << '\n';
        std::map<uint256, Json::Value> ret;
        Json::Value params;
        params[jss::account] = account.human();
        auto const& resp = env.rpc("json", "account_objects",
            to_string(params));
        std::cerr << "json:" << resp << '\n';
        Json::Value a(Json::arrayValue);
        a = resp[jss::result][jss::account_objects];
        for (auto const& object : a)
        {
            for (auto const& member : object.getMemberNames())
                std::cerr << "member:" << member << '\n';
            std::cerr << "LedgerEntryType:" << object["LedgerEntryType"] << '\n';
            if (object["LedgerEntryType"] != "PermissionedDomain")
                continue;
            uint256 index;
            std::ignore = index.parseHex(object[jss::index].asString());
            ret[index] = object;
            std::cerr << "object:" << object << '\n';
        }
        return ret;
    }

    static Blob
    toBlob(std::string const& input)
    {
        Blob ret;
        for (auto const& c : input)
            ret.push_back(c);
        return ret;
    }

    static std::string
    fromBlob(Blob const& input)
    {
        return {input.begin(), input.end()};
    }

    static Credentials
    credentialsFromJson(Json::Value const& object)
    {
        Credentials ret;
        Json::Value a(Json::arrayValue);
        a = object["AcceptedCredentials"];
        for (auto const& credential : a)
        {
            Json::Value obj(Json::objectValue);
            obj = credential["AcceptedCredential"];
            auto const issuer = obj["Issuer"];
            auto const credentialType = obj["CredentialType"];
            ret.push_back({AccountID(issuer.asString()),
                           strUnHex(credentialType.asString()).value()});
        }
        return ret;
    }

    static Credentials
    sortCredentials(Credentials const& input)
    {
        Credentials ret = input;
        std::sort(ret.begin(), ret.end(),
            [](Credential const& left, Credential const& right) -> bool {
                return left.first < right.first;
        });
        return ret;
    }

    static std::uint32_t
    ownerCount(Account const& account, Env& env)
    {
        Json::Value params;
        params[jss::account] = account.human();
        auto const& resp = env.rpc("json", "account_info",
            to_string(params));
        std::cerr << "account_info:" << resp << '\n';
        return env.rpc("json", "account_info",
            to_string(params))["result"]["account_data"]["OwnerCount"].asUInt();
    }

    // tests
    void
    testEnabled()
    {
        testcase("Enabled");
        Account const alice("alice");
        Env env(*this, withFeature_);
        env.fund(XRP(1000), alice);
        auto const setFee (drops(env.current()->fees().increment));
        Credentials credentials{{alice, toBlob("first credential")}};
        env(setTx(alice, credentials), fee(setFee));
        BEAST_EXPECT(ownerCount(alice, env) == 1);
        auto objects = getObjects(alice, env);
        BEAST_EXPECT(objects.size() == 1);
        auto const domain = objects.begin()->first;
        env(deleteTx(alice, domain));

        /*
        Account const alice{"alice"};
        Env env{*this, withFeature_};
        env.fund(XRP(1000), alice);
        auto const setFee {drops(env.current()->fees().increment)};
        Credentials credentials;
        credentials.emplace_back(alice, Blob());
        env(setTx(alice, credentials), fee(setFee));
        std::cerr << "ter:" << env.ter() << '\n';
        std::cerr << "tx:" << env.tx()->getJson(JsonOptions::none) << '\n';
        std::cerr << "meta:" << env.meta()->getJson(JsonOptions::none) << '\n';
        env.close();
        auto const objects = getObjects(alice, env);
        std::cerr << "number of objects: " << objects.size() << '\n';
        for (auto const& object : objects)
        {
            std::cerr << to_string(object.first) << ',' << object.second << '\n';
        }

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
            */
    }

    void
    testDisabled()
    {
        testcase("Disabled");
        Account const alice("alice");
        Env env(*this, withoutFeature_);
        env.fund(XRP(1000), alice);
        auto const setFee (drops(env.current()->fees().increment));
        Credentials credentials{{alice, toBlob("first credential")}};
        env(setTx(alice, credentials), fee(setFee), ter(temDISABLED));
        env(deleteTx(alice, uint256(75)), ter(temDISABLED));
        /*
        Account const alice{"alice"};
        Env env{*this, withoutFeature_};
        env.fund(XRP(1000), alice);
        auto const setFee {drops(env.current()->fees().increment)};
        env(deleteTx(alice, to_string(uint256(75))), ter(temDISABLED));
        env.close();
         */
    }

    void
    testSet()
    {
        testcase("Set");
        BEAST_EXPECT(true);
        {
            /*
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
                */
        }
    }

    void
    testDelete()
    {
        testcase("Delete");
        Env env(*this, withFeature_);
        Account const alice("alice");
        env.fund(XRP(1000), alice);
        auto const setFee(drops(env.current()->fees().increment));
        Credentials credentials{{alice, toBlob("first credential")}};
        env(setTx(alice, credentials), fee(setFee));
        env.close();
        auto objects = getObjects(alice, env);
        BEAST_EXPECT(objects.size() == 1);
        auto const domain = objects.begin()->first;

        // Delete a domain that doesn't belong to the account.
        Account const bob{"bob"};
        env.fund(XRP(1000), bob);
        env(deleteTx(bob, domain), ter(temINVALID_ACCOUNT_ID));
        env.close();

        // Delete a non-existent domain.
        env(deleteTx(alice, uint256(75)), ter(tecNO_ENTRY));

        // Delete domain that belongs to user.
        BEAST_EXPECT(ownerCount(alice, env) == 1);
        env(deleteTx(alice, domain), ter(tesSUCCESS));
        // Make sure we got the reserve back.
        BEAST_EXPECT(ownerCount(alice, env) == 0);

        //        env(deleteTx(alice, domain));
        /*
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
         */
    }

public:
    void
    run() override
    {
        testEnabled();
        testDisabled();
//        testSet();
        testDelete();
    }
};

BEAST_DEFINE_TESTSUITE_PRIO(PermissionedDomains, app, ripple, 2);

} // jtx
} // test
} // ripple
