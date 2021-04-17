//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2018 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_PERFTRACE_H
#define RIPPLE_BASICS_PERFTRACE_H

#include <ripple/basics/PerfLog.h>
#include <ripple/json/json_writer.h>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <utility>


namespace ripple {
namespace perf {

std::uint64_t
uniqueId();

#define STRING(line) #line
#define STRINGIFY(line) STRING(line)
#define FILE_LINE __FILE__ ":" STRINGIFY(__LINE__)

//------------------------------------------------------------------------------

class Tracer
{
private:
    // Used for reporting.
    Timers timers_;

    // These are used in generation of trace data.
    std::unordered_map<Timers::Timer::Tag,
                       std::chrono::steady_clock::time_point> timerTags_;
    std::mutex mutex_;

public:
    Tracer(std::string_view const& label, bool render = false)
        : timers_(std::chrono::steady_clock::now(), label, {}, {}, {},
                  render, {})
    {
        assert(label.size());
//        Json::Value tag{Json::objectValue};
//        timers_.timer.tag.toJson(tag);
//        std::cerr << "Tracer1: " << Json::Compact{std::move(tag)} << '\n';
    }

    Tracer(std::string_view const& label,
        std::pair<std::string_view, std::uint64_t> const& mutexTag,
           bool render = false)
        : timers_(std::chrono::steady_clock::now(), label, mutexTag.first,
                  mutexTag.second, {}, render, {})
    {
        assert(label.size());
//        Json::Value tag{Json::objectValue};
//        timers_.timer.tag.toJson(tag);
//        std::cerr << "Tracer2: " << Json::Compact{std::move(tag)} << '\n';
    }

    ~Tracer();

    Timers::Timer::Tag const&
    startTimer(Timers::Timer::Tag const& tag);

    void
    endTimer(Timers::Timer::Tag const& tag);
};

//------------------------------------------------------------------------------

template <class T>
class mutex
{
private:
    T mutex_;
    std::pair<std::string_view, std::uint64_t> const tag_;

public:
    mutex(std::string_view const& label)
        : tag_({label, uniqueId()})
    {
    }

    std::pair<std::string_view, std::uint64_t> const&
    tag() const
    {
        return tag_;
    }

    void
    lock()
    {
        mutex_.lock();
    }

    bool
    try_lock()
    {
        return mutex_.try_lock();
    }

    void
    unlock()
    {
        mutex_.unlock();
    }
};

template <class Mutex>
class lock_guard
{
private:
    Mutex& mutex_;
    std::string_view label_;
    std::shared_ptr<Tracer> tracer_;
    bool tracerFull_;
    Timers::Timer::Tag tracerTag_;

public:
    lock_guard(Mutex& mutex, std::string_view const& label,
               bool render = false, std::shared_ptr<Tracer> const& tracer = {})
        : mutex_(mutex)
        , label_(label)
        , tracer_(tracer)
        , tracerFull_(tracer)
    {
        auto const& tag = mutex_.tag();
        mutex_.lock();

        // Either append to an existing tracer object, or create a simple one.
        if (tracerFull_)
        {
            tracerTag_ = tracer_->startTimer(
                Timers::Timer::Tag({label, tag.first, tag.second}));
        }
        else
        {
            tracer_ = std::make_shared<Tracer>(label, tag, render);
        }
    }

    ~lock_guard()
    {
        mutex_.unlock();
        {
            if (tracerFull_)
                tracer_->endTimer(tracerTag_);
        }
    }
};

//------------------------------------------------------------------------------


std::shared_ptr<Tracer>
make_Tracer(std::string_view const& label, bool render = false);

std::shared_ptr<Tracer>
make_Tracer(std::string_view const& label, std::string_view const& mutexLabel,
    std::uint64_t const mutexId, bool render = false);

std::string_view const&
startTimer(std::shared_ptr<Tracer> const& tracer,
           std::string_view const& label);

inline std::string_view const&
startTimer(Tracer& tracer, std::string_view const& label)
{
    return tracer.startTimer(Timers::Timer::Tag(label)).label;
}

inline void
endTimer(std::shared_ptr<Tracer> const& tracer,
    std::string_view const& label)
{
    if (tracer)
        tracer->endTimer(Timers::Timer::Tag(label));
}

inline void
endTimer(Tracer& tracer, std::string_view const& label)
{
    tracer.endTimer(Timers::Timer::Tag(label));
}

#define TRACER Tracer(FILE_LINE)
#define TRACER_PTR make_Tracer(FILE_LINE)
#define TRACER_RENDER Tracer(FILE_LINE, true)
#define TRACER_RENDER_PTR make_Tracer(FILE_LINE, true)
#define TRACER_MUTEX(label, id) Tracer(FILE_LINE, label, id)
#define TRACER_MUTEX_PTR(label, id) make_Tracer( \
    FILE_LINE, label, id)
#define TRACER_MUTEX_RENDER(label, id) Tracer(FILE_LINE, label, id, true)
#define TRACER_MUTEX_RENDER_PTR(label, id) make_Tracer( \
    FILE_LINE, label, id, true)

#define START_TIMER(tracer) startTimer(tracer, FILE_LINE);
#define END_TIMER(tracer, label) endTimer(tracer, label);

#define LOCK_GUARD(mutex, lock) lock_guard lock(mutex, FILE_LINE)
#define LOCK_GUARD_RENDER(mutex, lock) lock_guard lock(mutex, FILE_LINE, true)
#define LOCK_GUARD_TRACER(mutex, tracer, lock) lock_guard lock(mutex, FILE_LINE, false, \
 tracer)
#define LOCK_GUARD_TRACER_RENDER(mutex, tracer) lock_guard(mutex, FILE_LINE, \
    true, tracer)

}  // namespace perf
}  // namespace ripple

#endif  // RIPPLE_BASICS_PERFTRACE_H
