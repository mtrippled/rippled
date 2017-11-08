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
#include <exception>
#include <iostream>
#if BEAST_LINUX
#include <sys/types.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>
#endif

namespace ripple {

Trace::~Trace()
{
    try
    {
        std::lock_guard<std::mutex> lock(mutex_);
        submit();
    }
    catch(std::exception& e)
    {
        std::cerr << "Trace ~ exception:\n" << e.what() << "\n";
        assert(false);
    }
}

Trace::Trace(Trace&& other)
{
    std::lock_guard<std::mutex> lock(other.mutex_);
    type_ = other.type_;
    other.type_ = perf::TraceType::none;
    events_ = std::move(other.events_);
    timers_ = std::move(other.timers_);
}

void Trace::add(std::string const& name,
        std::uint64_t const counter,
        perf::EventType const type)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (type_ == perf::TraceType::none)
        lockedOpen(name, counter, perf::TraceType::trace);
    type_ = perf::TraceType::trace;
    lockedAdd(name, type, counter);
}

void
Trace::start(std::string const& timer,
             std::uint64_t const counter,
             std::chrono::time_point<std::chrono::system_clock> const& tp)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (type_ == perf::TraceType::trace)
        lockedStart(timer, counter, tp);
}

void
Trace::end(std::string const& timer)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (type_ == perf::TraceType::trace)
        lockedEnd(timer);
}

void
Trace::close()
{
    std::lock_guard<std::mutex> lock(mutex_);
    lockedClose();
}

void
Trace::open(std::string const& name,
            std::uint64_t const counter,
            perf::TraceType const type)
{
    std::lock_guard<std::mutex> lock(mutex_);
    lockedOpen(name, counter, type);
}

//-----------------------------------------------------------------------------

void
Trace::lockedAdd(std::string const &name,
          perf::EventType const type,
          std::uint64_t const counter,
          std::chrono::time_point<std::chrono::system_clock> const &tp)
{
    assert(type_ != perf::TraceType::none);
    if (events_)
    {
        events_->emplace(tp,
            std::make_tuple(name,
                type,
#if BEAST_LINUX
                syscall(SYS_gettid),
#else
                std::this_thread::get_id(),
#endif
                counter));
    }
}

void
Trace::lockedStart(std::string const& timer,
                   std::uint64_t const counter,
                   std::chrono::time_point<std::chrono::system_clock> const& tp)
{
    assert(type_ == perf::TraceType::trace);
    timers_[timer] = tp;
    lockedAdd(timer, perf::EventType::start, counter, tp);
}

void
Trace::lockedEnd(std::string const& timer)
{
    assert(type_ == perf::TraceType::trace);

    auto start = timers_.find (timer);
    if (start != timers_.end())
    {
        std::chrono::time_point<std::chrono::system_clock> endTime =
                std::chrono::system_clock::now();
        std::uint64_t duration =
                std::chrono::duration_cast<std::chrono::microseconds> (
                        endTime - start->second).count();
        timers_.erase (start);
        lockedAdd(timer, perf::EventType::end, duration, endTime);
    }
}

bool
Trace::submit()
{
    if (type_ == perf::TraceType::none)
        return false;

    switch (type_)
    {
        case perf::TraceType::trace:
        {
            assert(events_ && !events_->empty());
            auto now = std::chrono::system_clock::now();
            lockedAdd("END", perf::EventType::generic,
                      std::chrono::duration_cast<std::chrono::microseconds>(
                              now - events_->begin()->first).count(), now);
        }
            break;
        case perf::TraceType::trap:
            assert(events_ && !events_->empty());
            break;
        default:
            assert(false);
    }

    perf::gPerfLog->addEvent(std::move(events_));

    return true;
}

void
Trace::lockedClose()
{
    if (!submit())
        return;
    timers_.clear();
    type_ = perf::TraceType::none;
}

void
Trace::lockedOpen(std::string const& name,
                  std::uint64_t const counter,
                  perf::TraceType const type)
{
    if (type == perf::TraceType::none)
        return;
    lockedClose();
    type_ = type;
    events_ = std::make_unique<perf::Events>();
    lockedAdd(name, perf::EventType::generic, counter);
    if (type == perf::TraceType::trap)
        lockedClose();
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

} // ripple
