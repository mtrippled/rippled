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

#ifndef RIPPLE_BASICS_TRACE_H
#define RIPPLE_BASICS_TRACE_H

#include <ripple/basics/PerfLog.h>
#include <memory>

namespace ripple {

namespace perf { enum class TraceType { trace, trap, timer }; }

class Trace
{
public:
    using pointer = std::shared_ptr<Trace>;
    using ref = pointer const&;

    Trace (std::string const& name,
           std::uint64_t const counter=0,
           perf::TraceType const type=perf::TraceType::trace)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            type_ = type;
        }
        open(name, counter, type, false);
    }

    ~Trace()
    {
        close(false);
    }

    Trace (Trace const& other)
    {
        std::unique_lock<std::mutex> thisLock(mutex_, std::defer_lock);
        std::unique_lock<std::mutex> otherLock(other.mutex_, std::defer_lock);
        std::lock(thisLock, otherLock);
        type_ = other.type_;
        events_ = other.events_;
        timers_ = other.timers_;
    }

    void add (std::string const& name,
              std::uint64_t const counter=0,
              perf::EventType const type=perf::EventType::generic)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        addEvent(name, type, counter);
    }

    void
    start (std::string const& timer,
           std::uint64_t const counter=0,
           std::chrono::time_point<std::chrono::system_clock> const& tp =
                   std::chrono::system_clock::now());

    void end (std::string const& timer);
    void close(bool clear=true);
    void open(std::string const& name,
              std::uint64_t const counter=0,
              perf::TraceType const type=perf::TraceType::trace,
              bool const doClose=true);

private:
    perf::TraceType type_;
    perf::Events events_;
    std::unordered_map<std::string,
            std::chrono::time_point<std::chrono::system_clock>> timers_;
    mutable std::mutex mutex_;

    void
    lockedStart(std::string const& timer,
                std::uint64_t const counter=0,
                std::chrono::time_point<std::chrono::system_clock> const& tp =
                std::chrono::system_clock::now());

    void
    addEvent(std::string const& name,
             perf::EventType const type,
             std::uint64_t const counter,
             std::chrono::time_point<std::chrono::system_clock> const& tp=
             std::chrono::system_clock::now())
    {
        events_.emplace(tp,
                        std::make_tuple(name,
                        type,
#if BEAST_LINUX
                        syscall(SYS_gettid),
#else
                        std::this_thread::get_id(),
#endif
                        counter));

    }

};

//-----------------------------------------------------------------------------

Trace::pointer
makeTrace(std::string const& name, std::uint64_t const counter=0);

void sendTrap(std::string const& name, std::uint64_t const counter=0);

Trace startTimer(std::string const& name, std::uint64_t const counter=0);

template <class T>
void
startTimer(T trace, std::string const& name, std::uint64_t const counter=0)
{
    if (trace)
        trace->start (name, counter);
}

template <class T>
void
endTimer(T trace, std::string const& name)
{
    if (trace)
        trace->end (name);
}

template <class T>
void
addTrace(T trace, std::string const& name, std::uint64_t const counter=0)
{
    if (trace)
        trace->add (name, counter);
}

} // ripple
#endif // RIPPLE_BASICS_TRACE_H
