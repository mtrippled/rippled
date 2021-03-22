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

#ifndef RIPPLE_BASICS_PERFLOG_H
#define RIPPLE_BASICS_PERFLOG_H

#include <ripple/basics/chrono.h>
#include <ripple/core/JobTypes.h>
#include <ripple/json/json_value.h>
#include <ripple/json/json_writer.h>
#include <ripple/json/to_string.h>
#include <ripple/protocol/jss.h>
#include <boost/core/ignore_unused.hpp>
#include <boost/filesystem.hpp>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <variant>

#if BEAST_LINUX
#include <sys/types.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>
#endif

namespace beast {
class Journal;
}

namespace ripple {
namespace perf_orig {

enum class PerfEventType
{
    generic,
    start,
    end,
    timer,
    aggregate
};

class Aggregate
{
protected:
    std::string name_;
    std::uint64_t counter_;

    static void
    aggregate(std::map<std::string, std::pair<std::uint64_t, std::uint64_t>>&
        aggregates, std::string const& name, std::uint64_t const counter)
    {
        auto agg = aggregates[name];
        ++agg.first;
        agg.second += counter;
    }

public:
    Aggregate(std::string const& name,
              std::uint64_t const counter)
        : name_(name)
        , counter_(counter)
    {}

    virtual ~Aggregate() {}

    virtual void
    render(std::chrono::time_point<std::chrono::system_clock> const& time,
           Json::Value& entry,
        std::map<std::string,
            std::pair<std::uint64_t, std::uint64_t>>& aggregates)
    {
        boost::ignore_unused(time);
        boost::ignore_unused(entry);
        aggregate(aggregates, name_, counter_);
    }
};

class Trap
    : public Aggregate
{
protected:
#if BEAST_LINUX
    int pid_;
#else
    std::thread::id tid_;
#endif

public:
    Trap(std::string const& name,
        std::uint64_t const counter)
        : Aggregate(name, counter)
        , tid_(
#if BEAST_LINUX
        syscall(SYS_gettid)
#else
        std::this_thread::get_id()
#endif
           )
    {}

    void
    render(std::chrono::time_point<std::chrono::system_clock> const& time,
        Json::Value& entry,
       std::map<std::string,
            std::pair<std::uint64_t, std::uint64_t>>& aggregates) override
    {
        entry[jss::type] = "trap";
        entry[jss::time] = to_string(time);
        entry[jss::name] = name_;
        entry[jss::counter] = std::to_string(counter_);
        std::stringstream ss;
        ss << tid_;
        entry[jss::tid] = ss.str();

        aggregate(aggregates, name_, counter_);
    }
};

class Trace
    : public Trap
{
private:
    using Event = std::tuple<std::string,
        PerfEventType,
#if BEAST_LINUX
        int,
#else
        std::thread::id,
#endif
        std::uint64_t>;
    using Events = std::multimap<
        std::chrono::time_point<std::chrono::system_clock>, Event>;
    std::multimap<std::chrono::time_point<std::chrono::system_clock>,
        Event> events_;
    std::mutex mutex_;

public:
    void
    render(std::chrono::time_point<std::chrono::system_clock> const& time,
        Json::Value& entry,
        std::map<std::string,
            std::pair<std::uint64_t, std::uint64_t>>& aggregates) override
    {
        entry[jss::type] = "trace";
        entry[jss::time] = to_string(time);
        entry[jss::name] = name_;
        entry[jss::counter] = std::to_string(counter_);
        std::stringstream ss;
        ss << tid_;
        entry[jss::tid] = ss.str();

        entry[jss::events] = Json::arrayValue;
        auto& events = entry[jss::events];

        for (auto const& e : events_)
        {
            auto const& name = std::get<0>(e.second);
            auto const& type = std::get<1>(e.second);
            auto const& tid = std::get<2>(e.second);
            auto const& counter = std::get<3>(e.second);
            Json::Value je = Json::objectValue;

            je[jss::time] = to_string(e.first);
            je[jss::name] = name;
            je[jss::counter] = std::to_string(counter);
#if BEAST_LINUX
            je[perf_jss::tid] = tid;
#else
            std::stringstream ss;
            ss << tid;
            je[jss::tid] = ss.str();
#endif
            if (type == PerfEventType::timer)
            {
                je[jss::type] = "timer";
                aggregate(aggregates, name, counter);
            }
            else
            {
                je[jss::type] = "generic";
            }

            events.append(je);
        }

        aggregate(aggregates, name_, counter_);
    }
};

using Event = std::variant<Aggregate, Trap, Trace>;

enum class PerfTraceType
{
    trace,
    trap,
    aggregate
};

class PerfEvents
{
public:
    using Event = std::tuple<std::string,
        PerfEventType,
#if BEAST_LINUX
        int,
#else
        std::thread::id,
#endif
        std::uint64_t>;
    using Events = std::multimap<
        std::chrono::time_point<std::chrono::system_clock>, Event>;

private:
    std::chrono::time_point<std::chrono::system_clock> time_;
    std::multimap<std::chrono::time_point<std::chrono::system_clock>,
        Event> events_;
    std::mutex mutex_;

public:
    PerfEvents()
        : time_ (std::chrono::system_clock::now())
    {}

    PerfEvents(PerfEvents const& other)
        : time_ (other.time_)
        , events_ (other.events_)
    {}

    void add (std::chrono::time_point<std::chrono::system_clock> const& time,
        std::string const& name,
        PerfEventType const type,
        std::uint64_t const counter)
    {
        std::lock_guard<std::mutex> lock (mutex_);

        events_.emplace(time, std::make_tuple(name,
            type,
#if BEAST_LINUX
            syscall(SYS_gettid),
#else
            std::this_thread::get_id(),
#endif
            counter));
    }

    void add (std::string const& name,
        PerfEventType const type=PerfEventType::generic,
        std::uint64_t const counter=0)
    {
        add(std::chrono::system_clock::now(), name, type, counter);
    }

    void
    aggregate()
    {
        assert(events_.size() == 1);
        auto entry = events_.begin();
        auto& event = entry->second;
        std::get<1>(event) = PerfEventType::aggregate;
        std::get<3>(event) =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - entry->first).count();
    }

    std::chrono::time_point<std::chrono::system_clock> const&
    getTime() const
    {
            return time_;
        }

    Events const&
    getEvents() const
    {
            return events_;
    }
};

//------------------------------------------------------------------------------

/**
 * Singleton class that maintains performance counters and optionally
 * writes Json-formatted data to a distinct log. It should exist prior
 * to other objects launched by Application to make it accessible for
 * performance logging.
 */

class PerfLog
{
public:
    using steady_clock = std::chrono::steady_clock;
    using system_clock = std::chrono::system_clock;
    using steady_time_point = std::chrono::time_point<steady_clock>;
    using system_time_point = std::chrono::time_point<system_clock>;
    using seconds = std::chrono::seconds;
    using milliseconds = std::chrono::milliseconds;
    using microseconds = std::chrono::microseconds;

    /**
     * Configuration from [perf_orig] section of rippled.cfg.
     */
    struct Setup
    {
        boost::filesystem::path perfLog;
        // log_interval is in milliseconds to support faster testing.
        milliseconds logInterval{seconds(1)};
    };

    virtual ~PerfLog() = default;

    /**
     * Log start of RPC call.
     *
     * @param method RPC command
     * @param requestId Unique identifier to track command
     */
    virtual void
    rpcStart(std::string const& method, std::uint64_t requestId) = 0;

    /**
     * Log successful finish of RPC call
     *
     * @param method RPC command
     * @param requestId Unique identifier to track command
     */
    virtual void
    rpcFinish(std::string const& method, std::uint64_t requestId) = 0;

    /**
     * Log errored RPC call
     *
     * @param method RPC command
     * @param requestId Unique identifier to track command
     */
    virtual void
    rpcError(std::string const& method, std::uint64_t requestId) = 0;

    /**
     * Log queued job
     *
     * @param type Job type
     */
    virtual void
    jobQueue(JobType const type) = 0;

    /**
     * Log job executing
     *
     * @param type Job type
     * @param dur Duration enqueued in microseconds
     * @param startTime Time that execution began
     * @param instance JobQueue worker thread instance
     */
    virtual void
    jobStart(
        JobType const type,
        microseconds dur,
        steady_time_point startTime,
        int instance) = 0;

    /**
     * Log job finishing
     *
     * @param type Job type
     * @param dur Duration running in microseconds
     * @param instance Jobqueue worker thread instance
     */
    virtual void
    jobFinish(JobType const type, microseconds dur, int instance) = 0;

    /**
     * Render performance counters in Json
     *
     * @return Counters Json object
     */
    virtual Json::Value
    countersJson() const = 0;

    /**
     * Render currently executing jobs and RPC calls and durations in Json
     *
     * @return Current executing jobs and RPC calls and durations
     */
    virtual Json::Value
    currentJson() const = 0;

    /**
     * Ensure enough room to store each currently executing job
     *
     * @param resize Number of JobQueue worker threads
     */
    virtual void
    resizeJobs(int const resize) = 0;

    /**
     * Rotate perf_orig log file
     */
    virtual void
    rotate() = 0;

    virtual void
    addEvent(PerfEvents const& event) = 0;
};

}  // namespace perf_orig

class Section;
class Stoppable;
class Application;

namespace perf_orig {

PerfLog::Setup
setup_PerfLog(Section const& section, boost::filesystem::path const& configDir);

std::unique_ptr<PerfLog>
make_PerfLog(
    PerfLog::Setup const& setup,
    Stoppable& parent,
    beast::Journal journal,
    std::function<void()>&& signalStop,
    Application* app);

extern PerfLog* perfLog;

}  // namespace perf_orig
}  // namespace ripple

#endif  // RIPPLE_BASICS_PERFLOG_H
