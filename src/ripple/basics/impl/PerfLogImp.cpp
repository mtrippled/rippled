//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2015 Ripple Labs Inc.

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

#include <ripple/basics/impl/PerfLogImp.h>
#include <ripple/basics/date.h>
#include <ripple/json/to_string.h>
#include <string>
#include <fstream>

namespace ripple {

PerfEvents makePerfEvents()
{
    return PerfEvents();
}

namespace perf {
PerfLog* perfLog = nullptr;
}

//------------------------------------------------------------------------------

PerfLogImp::PerfLogImp (std::string const& perf_log,
        unsigned int const log_interval,
        Stoppable& parent)
    : PerfLog (parent)
    , perf_log_ (perf_log)
    , log_interval_ (log_interval)
{
    openLog();
    perf::perfLog = this;
}

PerfLogImp::~PerfLogImp()
{
    report();

    if (thread_.joinable())
        thread_.join();
}

void PerfLogImp::reOpenLog()
{
    if (perf_log_.empty())
        return;

    if (perfLog_.is_open())
        perfLog_.close();

    openLog();
}

void PerfLogImp::run()
{
    beast::Thread::setCurrentThreadName ("perflog");
    lastLog_ = std::chrono::system_clock::now();

    while (true)
    {
        {
            std::unique_lock<std::mutex> lock (mutex_);

            if (stop_)
            {
                stopped();
                return;
            }

            if (rotate_)
                reOpenLog();

            if (perfGather_ && perfGather_->isStopping())
                perfGather_ = nullptr;

            cond_.wait_until (lock, lastLog_ +
                std::chrono::seconds (log_interval_));
        }

        report();
    }
}

void PerfLogImp::reportHeader (
    std::chrono::time_point<std::chrono::system_clock> const& now,
    Json::Value& report)
{
    report = Json::Value (Json::objectValue);
    report[perf_jss::time] = timeString (now);
    report[perf_jss::hostname] = cHostname_;
    report[perf_jss::pid] = cPid_;
    report[perf_jss::entries] = Json::Value (Json::arrayValue);
}

std::string PerfLogImp::timeString (
    std::chrono::time_point<std::chrono::system_clock> const& tp)
{
    auto tpu = ::date::floor<std::chrono::microseconds> (tp);
    auto dp = ::date::floor<days> (tpu);
    auto ymd = ::date::year_month_day (dp);
    auto time = ::date::make_time (tpu - dp);
    std::stringstream ss;
    ss << ymd << 'T' << time << 'Z';

    return ss.str();
}

void PerfLogImp::onStart()
{
    if (perf_log_.size())
        thread_ = std::thread (&PerfLogImp::run, this);
}

void PerfLogImp::onStop()
{
    if (perf_log_.size())
    {
        {
            std::lock_guard<std::mutex> lock (mutex_);
            stop_ = true;
        }
        cond_.notify_one();
    }
    else
    {
        stopped();
    }
}

void PerfLogImp::rotate()
{
    {
        std::lock_guard<std::mutex> lock (mutex_);
        rotate_ = true;
    }
    cond_.notify_one();
}

void PerfLogImp::report()
{
    if (perf_log_.empty())
        return;

    Json::Value report;
    auto now = std::chrono::system_clock::now();

    if (perfGather_ && now >=
            lastLog_ + std::chrono::seconds (log_interval_))
    {
        lastLog_ = now;

        reportHeader (now, report);
        report[perf_jss::num_workers] = perfGather_->getNumberOfThreads();
        auto& counters = perfGather_->counters();
        auto& entry = report[perf_jss::entries].append (Json::objectValue);
        entry[perf_jss::type] = "counters";
        entry[counters.jobQueueCounters.name()] =
                counters.jobQueueCounters.json();
        entry[counters.terCounters.name()] =
                counters.terCounters.json();

        auto nodeStoreCounters = perfGather_->nodeStore();
        entry[perf_jss::node_store_counters] = Json::arrayValue;
        for (size_t i=0; i < nodeStoreCounters.size(); ++i)
            entry[perf_jss::node_store_counters].append (std::to_string (
            nodeStoreCounters[i]));
    }

    std::multimap<std::chrono::time_point<std::chrono::system_clock>,
        PerfEvents> perfEvents;
    {
        std::lock_guard<std::mutex> lock (eventsMutex_);
        if (perfEvents_.size())
            perfEvents_.swap (perfEvents);
    }

    if (perfEvents.size())
    {
        if (report.type() == Json::nullValue)
            reportHeader (now, report);

        for (auto& trace : perfEvents)
        {
            auto& entry = report[perf_jss::entries].append (Json::objectValue);
            entry[perf_jss::type] = "trace";
            entry[perf_jss::time] = timeString (trace.second.time_);
            entry[perf_jss::events] = Json::arrayValue;

            auto& events = entry[perf_jss::events];
            for (auto& e : trace.second.events_)
            {
                Json::Value je = Json::objectValue;

                je[perf_jss::time] = timeString (e.first);
                je[perf_jss::name] = std::get<0> (e.second);
                switch (std::get<1> (e.second))
                {
                    case PerfEventType::generic:
                        je[perf_jss::type] = "generic";
                        break;
                    case PerfEventType::start:
                        je[perf_jss::type] = "start";
                        break;
                    case PerfEventType::end:
                        je[perf_jss::type] = "end";
                }
#if BEAST_LINUX
                je[perf_jss::tid] = get<2> (e.second);
#else
                std::stringstream ss;
                ss << std::get<2> (e.second);
                je[perf_jss::tid] = ss.str();
#endif
                je[perf_jss::counter] = std::to_string (std::get<3> (e.second));

                events.append (je);
            }
        }
    }

    if (report.type() != Json::nullValue)
        perfLog_ << Json::to_string (report) << std::endl;
}

void PerfLogImp::addEvent (PerfEvents const& event)
{
    if (perf_log_.size())
    {
        std::lock_guard<std::mutex> lock (eventsMutex_);
        perfEvents_.emplace (event.time_, event);
    }
}

//------------------------------------------------------------------------------

std::unique_ptr<PerfLog>
make_PerfLog (std::string const& perf_log,
        unsigned int const log_interval,
        beast::Stoppable& parent)
{
    return std::make_unique<PerfLogImp> (perf_log, log_interval, parent);
}

} // ripple
