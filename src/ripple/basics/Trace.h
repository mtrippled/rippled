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
           perf::TraceType const type=perf::TraceType::trace);

    ~Trace();

    Trace (Trace const& other)
            : type_ (other.type_)
            , events_ (other.events_)
            , timers_ (other.timers_)
    {}

    void add (std::string const& name,
              std::uint64_t const counter=0,
              perf::PerfEventType const type=perf::PerfEventType::generic)
    {
        events_.add (name, type, counter);
    }

    void start (std::string const& timer,
                std::chrono::time_point<std::chrono::system_clock>& tp,
                std::uint64_t const counter=0);

    void start (std::string const& timer,
                std::uint64_t const counter=0)
    {
        auto now = std::chrono::system_clock::now();
        start (timer, now, counter);
    }

    void end (std::string const& timer);

private:
    perf::TraceType type_;
    perf::PerfEvents events_;
    std::unordered_map<std::string,
    std::chrono::time_point<std::chrono::system_clock>> timers_;
    std::mutex timersMutex_;
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
