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

void
Trace::lockedStart(std::string const& timer,
                   std::uint64_t const counter,
                   std::chrono::time_point<std::chrono::system_clock> const& tp)
{
    assert(type_ != perf::TraceType::none);
    timers_[timer] = tp;
    if (type_ == perf::TraceType::trace)
        addEvent(timer, perf::EventType::start, counter, tp);
}


void
Trace::start(std::string const& timer,
             std::uint64_t const counter,
             std::chrono::time_point<std::chrono::system_clock> const& tp)
{
    std::lock_guard<std::mutex> lock(mutex_);
    lockedStart(timer, counter, tp);
}

void
Trace::end(std::string const& timer)
{
    std::uint64_t duration = 0;
    std::chrono::time_point<std::chrono::system_clock> endTime =
            std::chrono::system_clock::now();

    std::lock_guard<std::mutex> lock(mutex_);
    assert(type_ != perf::TraceType::none);
    auto start = timers_.find (timer);
    if (start != timers_.end())
    {
        duration = std::chrono::duration_cast<std::chrono::microseconds> (
                endTime - start->second).count();
        timers_.erase (start);
    }
    addEvent(timer, perf::EventType::end, duration, endTime);
}

void
Trace::close(bool clear)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (type_ != perf::TraceType::none)
    {
        switch (type_)
        {
            case perf::TraceType::trace:
            {
                assert(events_ && !events_->empty());
                auto now = std::chrono::system_clock::now();
                addEvent("END", perf::EventType::generic,
                        std::chrono::duration_cast<std::chrono::microseconds>(
                                now -
                                events_->begin()->first).count(), now);
            }
                break;
            case perf::TraceType::timer:
                assert(!timers_.empty());
                end(timers_.begin()->first);
                break;
            case perf::TraceType::none:
            case perf::TraceType::trap:
            default:
                assert(false);
        }

        if (events_)
            perf::gPerfLog->addEvent(std::move(events_));
    }
    if (clear)
    {
        if (events_)
            events_.reset();
        timers_.clear();
    }
    type_ = perf::TraceType::none;
}

void
Trace::open(std::string const& name,
            std::uint64_t const counter,
            perf::TraceType const type,
            bool const doClose)
{
    if (doClose)
        close();

    std::lock_guard<std::mutex> lock(mutex_);
    assert(type != perf::TraceType::none);
    events_.reset(new perf::Events);
    if (type_ != perf::TraceType::timer)
        addEvent(name, perf::EventType::generic, counter);
    else
        lockedStart(name);
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
