//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012-2014 Ripple Labs Inc.

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

#include <BeastConfig.h>
#include <ripple/app/main/Application.h>
#include <ripple/basics/Log.h>
#include <ripple/rpc/impl/Handler.h>

namespace ripple {

void
somefunc(std::unique_ptr<perf::Trace> const& trace=nullptr)
{
      perf::add(trace, "somefunc");
}

Json::Value doLogRotate (RPC::Context& context)
{
#if RIPPLED_PERF
    /*
    perf::Trace trace1("logrotation1");
    std::shared_ptr<perf::Trace> trace2 =
            std::make_shared<perf::Trace>("logrotation2");
    std::shared_ptr<perf::Trace> trace3;
    trace3.reset(new perf::Trace("logrotation3"));
    std::shared_ptr<perf::Trace> trace4 = perf::sharedTrace();
    perf::add(trace4, "crash");
    perf::open(trace4, "logrotation4");
    perf::close(trace4);
    perf::start(trace4, "football");
    perf::end(trace4, "football");
    perf::start(trace4, "thirty");
    perf::end(trace4, "fpar");
    trace4.reset();
    auto trace5 = perf::makeTrace("logrotation5");
    somefunc(trace5);
//    ripple::Trace trace6(std::move(trace1));
//    ripple::Trace trace6 = std::move(trace1);
//    trace6.add("hahahaha");
    perf::Trace trace7;
    trace7 = std::move(trace1);
    trace7.add("yohoho");
    perf::trap("itsatrap");
     */

    perf::gPerfLog->rotate();
#endif
    return RPC::makeObjectValue (context.app.logs().rotate());
}

} // ripple
