//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2015 Ripple Labs Inc.

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

#include <ripple/basics/PerfTrace.h>

namespace ripple {
namespace perf_orig {

PerfTrace::PerfTrace(
    std::string const& name,
    std::uint64_t const counter,
    PerfTraceType type)
    : perfLog_(*perf_orig::perfLog), type_(type), events_(PerfEvents())
{
    events_.add(events_.getTime(), name, PerfEventType::generic, counter);
}

PerfTrace::~PerfTrace()
{
    switch (type_)
    {
        case PerfTraceType::trace:
        {
            auto now = std::chrono::system_clock::now();
            events_.add(
                now,
                "END",
                PerfEventType::generic,
                std::chrono::duration_cast<std::chrono::microseconds>(
                    now - events_.getEvents().begin()->first)
                    .count());
            break;
        }
        case PerfTraceType::aggregate:
            events_.aggregate();
        case PerfTraceType::trap:
            assert(events_.getEvents().size() == 1);
            break;
        default:
            assert(false);
    }

    perfLog_.addEvent(events_);
}

void
PerfTrace::start(
    std::string const& timer,
    std::chrono::time_point<std::chrono::system_clock> const& tp,
    std::uint64_t const counter)
{
    {
        std::lock_guard<std::mutex> lock(timersMutex_);
        timers_[timer] = tp;
    }

    if (type_ == PerfTraceType::trace)
        events_.add(tp, timer, PerfEventType::start, counter);
}

void
PerfTrace::end(std::string const& timer)
{
    std::uint64_t duration = 0;
    std::chrono::time_point<std::chrono::system_clock> endTime =
        std::chrono::system_clock::now();

    {
        std::lock_guard<std::mutex> lock(timersMutex_);

        auto start = timers_.find(timer);
        if (start != timers_.end())
        {
            duration = std::chrono::duration_cast<std::chrono::microseconds>(
                           endTime - start->second)
                           .count();
            timers_.erase(start);
        }
    }

    events_.add(endTime, timer, PerfEventType::end, duration);
}

//------------------------------------------------------------------------------

PerfTrace::pointer
makeTrace(std::string const& name, std::uint64_t const counter,
          PerfTraceType type)
{
    return std::make_shared<PerfTrace>(name, counter);
}

void
sendTrap(std::string const& name, std::uint64_t const counter)
{
    PerfTrace(name, counter, PerfTraceType::trap);
}

std::unique_ptr<PerfTrace>
uniqueTrace(std::string const& name, std::uint64_t const counter,
            PerfTraceType type)
{
    return std::make_unique<PerfTrace>(name, counter, type);
}

} // perf_orig
} // ripple
