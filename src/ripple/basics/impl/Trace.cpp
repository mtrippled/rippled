//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2017 Ripple Labs Inc.

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

#include <ripple/basics/Trace.h>

namespace ripple {

Trace::Trace (std::string const& name,
              std::uint64_t const counter,
              perf::TraceType type)
        : type_ (type)
        , events_ (perf::PerfEvents())
{
    if (type_ != perf::TraceType::timer)
        events_.add(events_.time_, name, perf::PerfEventType::generic, counter);
    else
        start (name, events_.time_);
}

Trace::~Trace()
{
    switch (type_)
    {
        case perf::TraceType::trace:
        {
            auto now = std::chrono::system_clock::now();
            events_.add (now, "END", perf::PerfEventType::generic,
                         std::chrono::duration_cast<std::chrono::microseconds> (
                                 now - events_.events_.begin()->first).count());
        }
            break;
        case perf::TraceType::trap:
            break;
        case perf::TraceType::timer:
            end (timers_.begin()->first);
    }

    perf::gPerfLog->addEvent (events_);
}

void Trace::start (std::string const& timer,
                   std::chrono::time_point<std::chrono::system_clock>& tp,
                   std::uint64_t const counter)
{
    {
        std::lock_guard<std::mutex> lock (timersMutex_);
        timers_[timer] = tp;
    }

    if (type_ == perf::TraceType::trace)
        events_.add (tp, timer, perf::PerfEventType::start, counter);
}

void Trace::end (std::string const& timer)
{
    std::uint64_t duration = 0;
    std::chrono::time_point<std::chrono::system_clock> endTime =
            std::chrono::system_clock::now();

    {
        std::lock_guard<std::mutex> lock (timersMutex_);

        auto start = timers_.find (timer);
        if (start != timers_.end())
        {
            duration = std::chrono::duration_cast<std::chrono::microseconds> (
                    endTime - start->second).count();
            timers_.erase (start);
        }
    }

    events_.add (endTime, timer, perf::PerfEventType::end, duration);
}

//-----------------------------------------------------------------------------

std::shared_ptr<Trace>
makeTrace(std::string const& name, std::uint64_t const counter)
{
    return std::make_shared<Trace>(name, counter, perf::TraceType::trace);
}

void
sendTrap(std::string const& name, std::uint64_t const counter)
{
    Trace(name, counter, perf::TraceType::trap);
}

Trace
startTimer(std::string const& name, std::uint64_t const counter)
{
    return Trace(name, counter, perf::TraceType::timer);
}

} // ripple
