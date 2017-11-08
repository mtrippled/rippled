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

namespace perf { enum class TraceType { none = 0, trace, trap }; }

class Trace
{
public:
    using pointer = std::shared_ptr<Trace>;
    using ref = pointer const&;

    Trace() = default;

    Trace(std::string const& name,
          std::uint64_t const counter=0,
          perf::TraceType const type=perf::TraceType::trace)
    {
        lockedOpen(name, counter, type);
    }

    ~Trace();
    Trace(Trace const&) = delete;
    Trace(Trace&& other);

    explicit operator bool() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return type_ != perf::TraceType::none;
    }

    void add (std::string const& name,
            std::uint64_t const counter=0,
            perf::EventType const type=perf::EventType::generic);
    void start (std::string const& timer,
            std::uint64_t const counter=0,
            std::chrono::time_point<std::chrono::system_clock> const& tp =
                std::chrono::system_clock::now());
    void end (std::string const& timer);
    void close();
    void open(std::string const& name,
            std::uint64_t const counter=0,
            perf::TraceType const type=perf::TraceType::trace);

private:
    perf::TraceType type_ {perf::TraceType::none};
    std::unique_ptr<perf::Events> events_;
    std::unordered_map<std::string,
            std::chrono::time_point<std::chrono::system_clock>> timers_;
    mutable std::mutex mutex_;

    // These functions are called with mutex_ locked already as necessary.
    void lockedAdd(std::string const &name,
            perf::EventType const type,
            std::uint64_t const counter,
            std::chrono::time_point<std::chrono::system_clock> const &tp =
                std::chrono::system_clock::now());
    void lockedStart(std::string const& timer,
            std::uint64_t const counter=0,
            std::chrono::time_point<std::chrono::system_clock> const& tp =
                std::chrono::system_clock::now());
    void lockedEnd (std::string const& timer);
    bool submit();
    void lockedClose();
    void lockedOpen(std::string const& name,
         std::uint64_t const counter=0,
         perf::TraceType const type=perf::TraceType::trace);
};

//-----------------------------------------------------------------------------

Trace::pointer makeTrace(
        std::string const& name,
        std::uint64_t const counter=0);
void sendTrap(std::string const& name, std::uint64_t const counter=0);

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

} // ripple
#endif // RIPPLE_BASICS_TRACE_H
