//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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
#include <ripple/app/ledger/OpenLedger.h>
#include <ripple/app/tx/apply.h>
#include <ripple/ledger/CachedView.h>
#include <ripple/protocol/Feature.h>
#include <boost/range/adaptor/transformed.hpp>

namespace ripple {

OpenLedger::OpenLedger(std::shared_ptr<
    Ledger const> const& ledger,
        CachedSLEs& cache,
            beast::Journal journal)
    : j_ (journal)
    , cache_ (cache)
    , current_ (create(ledger->rules(), ledger))
{
}

bool
OpenLedger::empty() const
{
    std::lock_guard<
        std::mutex> lock(modify_mutex_);
    return current_->txCount() == 0;
}

std::shared_ptr<OpenView const>
OpenLedger::current() const
{
    std::lock_guard<
        std::mutex> lock(
            current_mutex_);
    return current_;
}

bool
#ifndef BENCHMARK
OpenLedger::modify (modify_type const& f, std::shared_ptr<OpenView const>& temp)
#else
OpenLedger::modify (modify_type const& f, std::shared_ptr<OpenView const>& temp,
    std::shared_ptr<PerfTrace> const& trace)
#endif
{
#ifdef BENCHMARK
    startTimer (trace, "acquire modify_mutex_");
#endif
    std::lock_guard<
        std::mutex> lock1(modify_mutex_);
#ifdef BENCHMARK
    endTimer (trace, "acquire modify_mutex_");
    startTimer (trace, "copy OpenView");
    current_->trace_ = trace;
#endif
    auto next = std::make_shared<
        OpenView>(*current_);
#ifdef BENCHMARK
    endTimer (trace, "copy OpenView");
    current_->trace_.reset();
    startTimer (trace, "modify callback");
#endif
    auto const changed = f(*next, j_);
#ifdef BENCHMARK
    endTimer (trace, "modify callback");
#endif
    if (changed)
    {
#ifdef BENCHMARK
        startTimer (trace, "acquire current_mutex_");
#endif
        std::lock_guard<
            std::mutex> lock2(
                current_mutex_);
#ifdef BENCHMARK
        endTimer (trace, "acquire current_mutex_");
        startTimer (trace, "move current_");
#endif
        temp = std::move(current_);
#ifdef BENCHMARK
        endTimer (trace, "move current_");
        startTimer (trace, "replace current_");
#endif
        current_ = std::move(next);
#ifdef BENCHMARK
        endTimer (trace, "replace current_");
#endif
    }
    return changed;
}

#ifdef BENCHMARK
bool
OpenLedger::modify (std::function<
    bool(OpenView&, beast::Journal)> const& f,
    std::shared_ptr<OpenView const>& temp)
{
    return modify (f, temp, std::shared_ptr<PerfTrace>());
}
#endif

void
OpenLedger::accept(Application& app, Rules const& rules,
    std::shared_ptr<Ledger const> const& ledger,
        OrderedTxs const& locals, bool retriesFirst,
            OrderedTxs& retries, ApplyFlags flags,
#ifdef BENCHMARK
            std::shared_ptr<PerfTrace> const& trace,
#endif
                std::string const& suffix,
                    modify_type const& f)
{
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 1");
#endif
    JLOG(j_.trace) <<
        "accept ledger " << ledger->seq() << " " << suffix;
    auto next = create(rules, ledger);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 1");
    startTimer (trace, "OpenLedger::accept 2");
#endif
    if (retriesFirst)
    {
        // Handle disputed tx, outside lock
        using empty =
            std::vector<std::shared_ptr<
                STTx const>>;
        apply (app, *next, *ledger, empty{},
            retries, flags, j_);
    }
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 2");
#endif
    // Block calls to modify, otherwise
    // new tx going into the open ledger
    // would get lost.
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 3");
#endif
    std::lock_guard<
        std::mutex> lock1(modify_mutex_);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 3");
#endif
    // Apply tx from the current open view
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 4");
#endif
    if (! current_->txs.empty())
        apply (app, *next, *ledger,
            boost::adaptors::transform(
                current_->txs,
            [](std::pair<std::shared_ptr<
                STTx const>, std::shared_ptr<
                    STObject const>> const& p)
            {
                return p.first;
            }),
                retries, flags, j_);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 4");
#endif
    // Apply local tx
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 5");
#endif
    for (auto const& item : locals)
        ripple::apply(app, *next,
            *item.second, flags, j_);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 5");
#endif
    // Call the modifier
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 6");
#endif
    if (f)
        f(*next, j_);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 6");
#endif
    // Switch to the new open view
#ifdef BENCHMARK
    startTimer (trace, "OpenLedger::accept 7");
#endif
    std::lock_guard<
        std::mutex> lock2(current_mutex_);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 7");
    startTimer (trace, "OpenLedger::accept 8");
#endif
    current_ = std::move(next);
#ifdef BENCHMARK
    endTimer (trace, "OpenLedger::accept 8");
#endif
}

//------------------------------------------------------------------------------

std::shared_ptr<OpenView>
OpenLedger::create (Rules const& rules,
    std::shared_ptr<Ledger const> const& ledger)
{
    return std::make_shared<OpenView>(
        open_ledger, rules, std::make_shared<
            CachedLedger const>(ledger,
                cache_));
}

auto
OpenLedger::apply_one (Application& app, OpenView& view,
    std::shared_ptr<STTx const> const& tx,
        bool retry, ApplyFlags flags,
            beast::Journal j) -> Result
{
    if (retry)
        flags = flags | tapRETRY;
    auto const result = ripple::apply(
        app, view, *tx, flags, j);
    if (result.second)
        return Result::success;
    if (isTefFailure (result.first) ||
        isTemMalformed (result.first) ||
            isTelLocal (result.first))
        return Result::failure;
    return Result::retry;
}

//------------------------------------------------------------------------------

std::string
debugTxstr (std::shared_ptr<STTx const> const& tx)
{
    std::stringstream ss;
    ss << tx->getTransactionID();
    return ss.str().substr(0, 4);
}

std::string
debugTostr (OrderedTxs const& set)
{
    std::stringstream ss;
    for(auto const& item : set)
        ss << debugTxstr(item.second) << ", ";
    return ss.str();
}

std::string
debugTostr (SHAMap const& set)
{
    std::stringstream ss;
    for (auto const& item : set)
    {
        try
        {
            SerialIter sit(item.slice());
            auto const tx = std::make_shared<
                STTx const>(sit);
            ss << debugTxstr(tx) << ", ";
        }
        catch(std::exception const&)
        {
            ss << "THRO, ";
        }
    }
    return ss.str();
}

std::string
debugTostr (std::shared_ptr<ReadView const> const& view)
{
    std::stringstream ss;
    for(auto const& item : view->txs)
        ss << debugTxstr(item.first) << ", ";
    return ss.str();
}

} // ripple
