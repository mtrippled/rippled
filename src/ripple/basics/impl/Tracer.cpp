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

#include <ripple/basics/Tracer.h>
#include <atomic>

namespace ripple {
namespace perf {

Tracer::~Tracer()
{
    std::lock_guard<std::mutex> lock(mutex_);
    timers_.timer.duration_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - timers_.timer.start_time);
    perfLog->addEvent(timers_);
}

std::uint64_t
uniqueId()
{
    static std::atomic<std::uint64_t> counter{1};
    return counter.fetch_add(1);
}

std::shared_ptr<Tracer>
make_Tracer(std::string_view const& label, bool render)
{
    return std::make_unique<Tracer>(label, render);
}

Timers::Timer::Tag const&
Tracer::startTimer(Timers::Timer::Tag const& tag)
{
    std::lock_guard<std::mutex> lock(mutex_);
    timerTags_[tag] = std::chrono::steady_clock::now();
    return tag;
}

void
Tracer::endTimer(Timers::Timer::Tag const& tag)
{
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mutex_);
    auto start = timerTags_.find(tag);
    if (start == timerTags_.end())
        return;

    timers_.sub_timers.push_back({start->second, start->first.label,
                                  start->first.mutex_label,
                                  start->first.mutex_id,
                                  std::chrono::duration_cast<std::chrono::microseconds>(
                                      now - start->second)});
    timerTags_.erase(start);
}

std::string_view const&
startTimer(std::shared_ptr<Tracer> const& tracer,
    std::string_view const& label)
{
    if (tracer)
        tracer->startTimer(Timers::Timer::Tag(label));
    return label;
}

}  // namespace perf
}  // namespace ripple
