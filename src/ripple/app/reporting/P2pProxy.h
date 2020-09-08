//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2020 Ripple Labs Inc.

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

#ifndef RIPPLE_CORE_TXPROXY_H_INCLUDED
#define RIPPLE_CORE_TXPROXY_H_INCLUDED

#include <ripple/app/main/Application.h>
#include <ripple/rpc/Context.h>
#include <ripple/rpc/impl/Handler.h>

#include <boost/beast/websocket.hpp>

#include "org/xrpl/rpc/v1/xrp_ledger.grpc.pb.h"
#include <grpcpp/grpcpp.h>

namespace ripple {
Json::Value
forwardToP2p(RPC::JsonContext& context);

bool
shouldForwardToP2p(RPC::JsonContext& context);

template <class T>
struct SpecifiesLedger
{
    // List out all GRPC request types that specify a ledger
    // Note, GetAccountTransactionHistory specifies a ledger, but
    // GetAccountTransationHistory only ever returns validated data, so
    // GetAccountTransactionHistory will never be forwarded
    static bool const value =
        std::is_same<T, org::xrpl::rpc::v1::GetAccountInfoRequest>::value ||
        std::is_same<T, org::xrpl::rpc::v1::GetLedgerRequest>::value ||
        std::is_same<T, org::xrpl::rpc::v1::GetLedgerDataRequest>::value;
};

template <
    class Request,
    typename std::enable_if<!SpecifiesLedger<Request>::value, Request>::type* =
        nullptr>
bool
needCurrentOrClosed(RPC::GRPCContext<Request>& context)
{
    return false;
}

template <
    class Request,
    typename std::enable_if<SpecifiesLedger<Request>::value, Request>::type* =
        nullptr>
bool
needCurrentOrClosed(RPC::GRPCContext<Request>& context)
{
    // TODO consider forwarding sequence values greater than the latest
    // sequence we have
    if (context.params.ledger().ledger_case() ==
        org::xrpl::rpc::v1::LedgerSpecifier::LedgerCase::kShortcut)
    {
        if (context.params.ledger().shortcut() !=
            org::xrpl::rpc::v1::LedgerSpecifier::SHORTCUT_VALIDATED)
            return true;
    }
    return false;
}

template <class Request>
bool
shouldForwardToP2p(RPC::GRPCContext<Request>& context, RPC::Condition condition)
{
    if (!context.app.config().reporting())
        return false;
    if (condition == RPC::NEEDS_CURRENT_LEDGER ||
        condition == RPC::NEEDS_CLOSED_LEDGER)
        return true;

    return needCurrentOrClosed(context);
}

std::unique_ptr<org::xrpl::rpc::v1::XRPLedgerAPIService::Stub>
getP2pForwardingStub(RPC::Context& context);

}  // namespace ripple
#endif
