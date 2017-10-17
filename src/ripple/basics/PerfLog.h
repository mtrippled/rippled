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

#ifndef RIPPLE_BASICS_PERFLOG_H_INCLUDED
#define RIPPLE_BASICS_PERFLOG_H_INCLUDED

#include <ripple/protocol/TER.h>
#include <ripple/core/Job.h>
#include <ripple/beast/core/PlatformConfig.h>
#include <string>
#include <memory>
#include <mutex>
#include <map>
#include <tuple>
#include <thread>
#if BEAST_LINUX
#include <sys/types.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <sys/syscall.h>
#endif

namespace ripple {

namespace perf {

enum class PerfEventType
{
    generic,
    start,
    end
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

    PerfEvents()
            : time_ (std::chrono::system_clock::now())
    {}

    PerfEvents(PerfEvents const& other)
            : time_ (other.time_)
            , events_ (other.events_)
    {}

    void add(std::chrono::time_point<std::chrono::system_clock> const& time,
              std::string const& name,
              PerfEventType const type,
              std::uint64_t const counter)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        events_.emplace(time, std::make_tuple (name,
                                               type,
#if BEAST_LINUX
                                               syscall(SYS_gettid),
#else
                                               std::this_thread::get_id(),
#endif
                                               counter));
    }

    void add(std::string const& name,
             PerfEventType const type=PerfEventType::generic,
             std::uint64_t const counter=0)
    {
        add(std::chrono::system_clock::now(), name, type, counter);
    }

    std::chrono::time_point<std::chrono::system_clock> time_;
    std::multimap<std::chrono::time_point<std::chrono::system_clock>,
            Event> events_;
    std::mutex mutex_;
    };

//-----------------------------------------------------------------------------

/**
 * This class logs performance data, and also maintains configuration options
 * for benchmarking.
 */
class PerfLog
{
public:
    struct Setup
    {
        std::string perf_log;
        unsigned int log_interval{1};
    };

    virtual ~PerfLog() {}

    /** Re-open logfile for RPC "logrotate". */
    virtual void rotate() = 0;
    virtual void addEvent(PerfEvents const &event) = 0;
    virtual void rpcRunning(std::string const &method) = 0;
    virtual void rpcFinished(std::string const &method) = 0;
    virtual void rpcErrored(std::string const &method) = 0;
    virtual void jobQueued(JobType const &jt) = 0;
    virtual void jobRunning(JobType const &jt) = 0;
    virtual void jobFinished(JobType const &jt) = 0;
    virtual void ter(TER const& ter) = 0;
    virtual void setNumberOfThreads(int const workers) = 0;
};

//------------------------------------------------------------------------------

// All Trace objects need to find this.
extern PerfLog* gPerfLog;

} // perf

class Section;
/** Build PerfLog::Setup from a config section. */
perf::PerfLog::Setup
setup_PerfLog(Section const& section);

class Stoppable;
class Application;

std::unique_ptr<perf::PerfLog>
make_PerfLog(perf::PerfLog::Setup const& setup,
             Stoppable& parent,
             Application& app);

} // ripple

#endif // RIPPLE_BASICS_PERFLOG_H_INCLUDED
