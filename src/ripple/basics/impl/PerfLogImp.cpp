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

#include <ripple/app/main/Application.h>
#include <ripple/basics/BasicConfig.h>
#include <ripple/basics/impl/PerfLogImp.h>
#include <ripple/beast/core/CurrentThreadName.h>
#include <ripple/beast/utility/Journal.h>
#include <ripple/core/JobTypes.h>
#include <ripple/json/json_writer.h>
#include <ripple/json/to_string.h>
#include <ripple/nodestore/DatabaseShard.h>
#include <ripple/overlay/Overlay.h>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <iterator>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace ripple {
namespace perf {

PerfLog* perfLog = nullptr;

PerfLogImp::Counters::Counters(
    std::vector<char const*> const& labels,
    JobTypes const& jobTypes,
    Application& app)
    : app_(app)
{
    {
        // populateRpc
        rpc_.reserve(labels.size());
        for (std::string const label : labels)
        {
            auto const inserted = rpc_.emplace(label, Rpc()).second;
            if (!inserted)
            {
                // Ensure that no other function populates this entry.
                assert(false);
            }
        }
    }
    {
        // populateJq
        jq_.reserve(jobTypes.size());
        for (auto const& [jobType, _] : jobTypes)
        {
            auto const inserted = jq_.emplace(jobType, Jq()).second;
            if (!inserted)
            {
                // Ensure that no other function populates this entry.
                assert(false);
            }
        }
    }
}

Json::Value
PerfLogImp::Counters::countersJson() const
{
    Json::Value rpcobj(Json::objectValue);
    // totalRpc represents all rpc methods. All that started, finished, etc.
    Rpc totalRpc;
    for (auto const& proc : rpc_)
    {
        Rpc value;
        {
            std::lock_guard lock(proc.second.mutex);
            if (!proc.second.value.started && !proc.second.value.finished &&
                !proc.second.value.errored)
            {
                continue;
            }
            value = proc.second.value;
        }

        Json::Value p(Json::objectValue);
        p[jss::started] = std::to_string(value.started);
        totalRpc.started += value.started;
        p[jss::finished] = std::to_string(value.finished);
        totalRpc.finished += value.finished;
        p[jss::errored] = std::to_string(value.errored);
        totalRpc.errored += value.errored;
        p[jss::duration_us] = std::to_string(value.duration.count());
        totalRpc.duration += value.duration;
        rpcobj[proc.first] = p;
    }

    if (totalRpc.started)
    {
        Json::Value totalRpcJson(Json::objectValue);
        totalRpcJson[jss::started] = std::to_string(totalRpc.started);
        totalRpcJson[jss::finished] = std::to_string(totalRpc.finished);
        totalRpcJson[jss::errored] = std::to_string(totalRpc.errored);
        totalRpcJson[jss::duration_us] =
            std::to_string(totalRpc.duration.count());
        rpcobj[jss::total] = totalRpcJson;
    }

    Json::Value jqobj(Json::objectValue);
    // totalJq represents all jobs. All enqueued, started, finished, etc.
    Jq totalJq;
    for (auto const& proc : jq_)
    {
        Jq value;
        {
            std::lock_guard lock(proc.second.mutex);
            if (!proc.second.value.queued && !proc.second.value.started &&
                !proc.second.value.finished)
            {
                continue;
            }
            value = proc.second.value;
        }

        Json::Value j(Json::objectValue);
        j[jss::queued] = std::to_string(value.queued);
        totalJq.queued += value.queued;
        j[jss::started] = std::to_string(value.started);
        totalJq.started += value.started;
        j[jss::finished] = std::to_string(value.finished);
        totalJq.finished += value.finished;
        j[jss::queued_duration_us] =
            std::to_string(value.queuedDuration.count());
        totalJq.queuedDuration += value.queuedDuration;
        j[jss::running_duration_us] =
            std::to_string(value.runningDuration.count());
        totalJq.runningDuration += value.runningDuration;
        jqobj[JobTypes::name(proc.first)] = j;
    }

    if (totalJq.queued)
    {
        Json::Value totalJqJson(Json::objectValue);
        totalJqJson[jss::queued] = std::to_string(totalJq.queued);
        totalJqJson[jss::started] = std::to_string(totalJq.started);
        totalJqJson[jss::finished] = std::to_string(totalJq.finished);
        totalJqJson[jss::queued_duration_us] =
            std::to_string(totalJq.queuedDuration.count());
        totalJqJson[jss::running_duration_us] =
            std::to_string(totalJq.runningDuration.count());
        jqobj[jss::total] = totalJqJson;
    }

    Json::Value nodestore(Json::objectValue);
    if (app_.getShardStore())
        app_.getShardStore()->getCountsJson(nodestore);
    else
        app_.getNodeStore().getCountsJson(nodestore);

    Json::Value counters(Json::objectValue);
    // Be kind to reporting tools and let them expect rpc and jq objects
    // even if empty.
    counters[jss::rpc] = rpcobj;
    counters[jss::job_queue] = jqobj;
    counters[jss::nodestore] = nodestore;
    return counters;
}

Json::Value
PerfLogImp::Counters::currentJson() const
{
    auto const present = steady_clock::now();

    Json::Value jobsArray(Json::arrayValue);
    auto const jobs = [this] {
        std::lock_guard lock(jobsMutex_);
        return jobs_;
    }();

    for (auto const& j : jobs)
    {
        if (j.first == jtINVALID)
            continue;
        Json::Value jobj(Json::objectValue);
        jobj[jss::job] = JobTypes::name(j.first);
        jobj[jss::duration_us] = std::to_string(
            std::chrono::duration_cast<microseconds>(present - j.second)
                .count());
        jobsArray.append(jobj);
    }

    Json::Value methodsArray(Json::arrayValue);
    std::vector<MethodStart> methods;
    {
        std::lock_guard lock(methodsMutex_);
        methods.reserve(methods_.size());
        for (auto const& m : methods_)
            methods.push_back(m.second);
    }
    for (auto m : methods)
    {
        Json::Value methodobj(Json::objectValue);
        methodobj[jss::method] = m.first;
        methodobj[jss::duration_us] = std::to_string(
            std::chrono::duration_cast<microseconds>(present - m.second)
                .count());
        methodsArray.append(methodobj);
    }

    Json::Value current(Json::objectValue);
    current[jss::jobs] = jobsArray;
    current[jss::methods] = methodsArray;
    return current;
}

//-----------------------------------------------------------------------------

void
PerfLogImp::openLog()
{
    if (setup_.perfLog.empty())
        return;

    if (logFile_.is_open())
        logFile_.close();

    auto logDir = setup_.perfLog.parent_path();
    if (!boost::filesystem::is_directory(logDir))
    {
        boost::system::error_code ec;
        boost::filesystem::create_directories(logDir, ec);
        if (ec)
        {
            JLOG(j_.fatal()) << "Unable to create performance log "
                                "directory "
                             << logDir << ": " << ec.message();
            signalStop_();
            return;
        }
    }

    logFile_.open(setup_.perfLog.c_str(), std::ios::out | std::ios::app);

    if (!logFile_)
    {
        JLOG(j_.fatal()) << "Unable to open performance log " << setup_.perfLog
                         << ".";
        signalStop_();
    }
}

void
PerfLogImp::run()
{
    beast::setCurrentThreadName("perflog");
    lastLog_ = system_clock::now();

    while (true)
    {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (cond_.wait_until(
                    lock, lastLog_ + setup_.logInterval, [&] { return stop_; }))
            {
                return;
            }
            if (rotate_)
            {
                openLog();
                rotate_ = false;
            }
        }
        report();
    }
}

struct Aggregate
{
    std::uint64_t count{0};
    std::chrono::microseconds duration_us{0};
    std::chrono::microseconds avg_us{0};
    double percent{0};

    bool
    operator<(Aggregate const& other) const
    {
        return duration_us < other.duration_us;
    }

    Json::Value
    toJson(std::optional<const Aggregate> parent = {}) const
    {
        assert(count);
        Json::Value ret{Json::objectValue};
        ret[jss::count] = std::to_string(count);
        ret[jss::duration_us] = std::to_string(duration_us.count());
        ret[jss::avg_us] =  static_cast<double>(duration_us.count() / count);

        double totalDuration;
        if (parent)
            totalDuration = parent->duration_us.count();
        else
            totalDuration = duration_us.count();
        if (totalDuration)
        {
            ret[jss::percent] = static_cast<double>(
                duration_us.count() * 100 / totalDuration);
        }
        else // could happen with granular timer samples
        {
            ret[jss::percent] = 0.0;
        }

        return ret;
    }
};

Json::Value
PerfLogImp::reportEvents()
{
    std::vector<Timers> events;
    {
        std::lock_guard<std::mutex> lock(eventsMutex_);
        events_.swap(events);
    }
    if (events.empty())
        return {};

    using IntermediateMap = std::unordered_map<Timers::Timer::Tag,
          std::pair<Aggregate,
                    std::unordered_map<Timers::Timer::Tag, Aggregate>>>;
    IntermediateMap tracerIntermediates;
    IntermediateMap mutexIntermediates;

    Json::Value detailJson{Json::arrayValue};
    for (auto const& event : events)
    {
        Json::Value traceJson;

        if (event.render)
            traceJson = event.timer.toJson();

        std::cerr << __FILE__ << __LINE__ << "cerr string,size: "
                  << event.timer.tag.mutex_label << ','
                  << event.timer.tag.mutex_label.size() << '\n';
        auto& tracerIntermediate = tracerIntermediates[event.timer.tag];
        std::cerr << __FILE__ << __LINE__ << "cerr mapsize: " << tracerIntermediates.size() << '\n';
        ++tracerIntermediate.first.count;
        tracerIntermediate.first.duration_us += event.timer.duration_us;

        if (event.timer.tag.mutex_id)
        {
            Timers::Timer::Tag mutexTag = event.timer.tag;
            // Remove the lock label from the mutex tag.
            // This is the aggregation of all locks of the mutex.
            mutexTag.label = "";
            auto& mutexIntermediate = mutexIntermediates[mutexTag];
            ++mutexIntermediate.first.count;
            mutexIntermediate.first.duration_us += event.timer.duration_us;
            auto& lock = mutexIntermediate.second[event.timer.tag];
            ++lock.count;
            lock.duration_us += event.timer.duration_us;
        }

        traceJson["sub_timers_size"] = std::to_string(event.sub_timers.size());
        Json::Value subTraceJson(Json::arrayValue);
        for (auto const& subTimer : event.sub_timers)
        {
            if (event.render)
                subTraceJson.append(subTimer.toJson());

            auto& subAggregate = tracerIntermediate.second[subTimer.tag];
            ++subAggregate.count;
            subAggregate.duration_us += subTimer.duration_us;

            if (subTimer.tag.mutex_id)
            {
                Timers::Timer::Tag mutexTag = subTimer.tag;
                // Remove the lock label from the mutex tag.
                // This is the aggregation of all locks of the mutex.
                mutexTag.label = "";
                auto& mutexIntermediate = mutexIntermediates[mutexTag];
                ++mutexIntermediate.first.count;
                mutexIntermediate.first.duration_us += subTimer.duration_us;
                auto& lockAggregate = mutexIntermediate.second[subTimer.tag];
                ++lockAggregate.count;
                lockAggregate.duration_us += subTimer.duration_us;
            }
        }

        if (subTraceJson.size())
            traceJson[jss::subtimers] = subTraceJson;
        if (event.render)
            detailJson.append(traceJson);
    }

    using EndMap = std::map<Timers::Timer::Tag,
        std::pair<Aggregate, std::multimap<Aggregate, Timers::Timer::Tag>>>;
    EndMap tracerEnds;
    EndMap mutexEnds;

    std::cerr << "cerr mapsize: " << tracerIntermediates.size() << '\n';
    for (auto const& tracerIntermediate : tracerIntermediates)
    {
        std::cerr << __FILE__ << __LINE__ << "cerr string,size: "
                  << tracerIntermediate.first.mutex_label << ','
                  << tracerIntermediate.first.mutex_label.size() << '\n';
        auto& endTracer = tracerEnds[tracerIntermediate.first];
        endTracer.first = tracerIntermediate.second.first;
        for (auto& subTimer : tracerIntermediate.second.second)
            endTracer.second.insert({subTimer.second, subTimer.first});
    }
    for (auto& mutexIntermediate : mutexIntermediates)
    {
        auto& endMutex = mutexEnds[mutexIntermediate.first];
        endMutex.first = mutexIntermediate.second.first;
        for (auto& lock : mutexIntermediate.second.second)
            endMutex.second.insert({lock.second, lock.first});
    }

    Json::Value tracesJson{Json::objectValue};
    for (auto const& tracer : tracerEnds)
    {
        tracesJson[tracer.first.tracer()] = Json::objectValue;
        Json::Value& traceJson = tracesJson[tracer.first.tracer()];
        Aggregate const& parentStats = tracer.second.first;
        traceJson[jss::stats] = parentStats.toJson();

        Json::Value subTimersJson{Json::arrayValue};
        std::multimap<Aggregate,
                      Timers::Timer::Tag>::const_reverse_iterator rit;
        for (rit = tracer.second.second.rbegin();
             rit != tracer.second.second.rend();
             ++rit)
        {
            Json::Value subTimerJson{Json::objectValue};
            subTimerJson[jss::label] = rit->second.subTracer();
            subTimerJson[jss::stats] = rit->first.toJson(parentStats);
            subTimersJson.append(subTimerJson);
        }
        if (subTimersJson.size())
            traceJson[jss::subtimers] = subTimersJson;
    }

    Json::Value mutexesJson{Json::objectValue};
    for (auto const& mutex : mutexEnds)
    {
        mutexesJson[mutex.first.mutex()] = Json::objectValue;
        Json::Value& mutexJson = mutexesJson[mutex.first.mutex()];
        Aggregate const& parentStats = mutex.second.first;
        mutexJson[jss::stats] = parentStats.toJson();

        Json::Value locksJson{Json::arrayValue};
        std::multimap<Aggregate,
            Timers::Timer::Tag>::const_reverse_iterator rit;
        for (rit = mutex.second.second.rbegin();
             rit != mutex.second.second.rend();
             ++rit)
        {
            Json::Value lockJson{Json::objectValue};
            lockJson[jss::label] = rit->second.subMutex();
            lockJson[jss::stats] = rit->first.toJson(parentStats);
            locksJson.append(lockJson);
        }
        if (locksJson.size())
            mutexJson[jss::locks] = locksJson;
    }

    Json::Value statsJson{Json::objectValue};
    if (tracesJson.size())
        statsJson[jss::traces] = tracesJson;
    if (mutexesJson.size())
        statsJson[jss::mutex] = mutexesJson;

    Json::Value ret{Json::objectValue};
    if (statsJson.size())
        ret[jss::stats] = statsJson;
    if (detailJson.size())
        ret[jss::detail] = detailJson;

    return ret;
}

void
PerfLogImp::report()
{
    if (!logFile_)
        // If logFile_ is not writable do no further work.
        return;

    auto const present = system_clock::now();
    auto const steadyPresent = steady_clock::now();
    if (present < lastLog_ + setup_.logInterval)
        return;
    lastLog_ = present;

    Json::Value report(Json::objectValue);
    report[jss::time] = to_string(date::floor<microseconds>(present));
    {
        std::lock_guard lock{counters_.jobsMutex_};
        report[jss::workers] =
            static_cast<unsigned int>(counters_.jobs_.size());
    }
    report[jss::hostid] = hostname_;
    report[jss::counters] = counters_.countersJson();
    report[jss::current_activities] = counters_.currentJson();

    report[jss::jq_trans_overflow] =
        std::to_string(app_.overlay().getJqTransOverflow());
    report[jss::peer_disconnects] =
        std::to_string(app_.overlay().getPeerDisconnect());
    report[jss::peer_disconnects_resources] =
        std::to_string(app_.overlay().getPeerDisconnectCharges());


    /*
    std::vector<Timers> events;
    {
        std::lock_guard lock(eventsMutex_);
        events.swap(events_);
    }

    std::map<std::string_view,
        std::pair<Aggregate, std::unordered_map<Timers::Timer::Tag,
                Aggregate>>> aggregates;
    std::map<std::string,
        std::pair<Aggregate, std::unordered_map<std::string_view,
            Aggregate>>> mutexAggregates;

    if (events.size())
    {
        Json::Value tracesJson{Json::objectValue};
        Json::Value eventsJson{Json::arrayValue};
        for (auto const& event : events)
        {
            Json::Value traceJson(Json::arrayValue);
            if (event.render)
                traceJson.append(event.timer.toJson());

            auto& agg = aggregates[event.timer.tag.label];
            ++agg.first.count;
            ++agg.first.duration_us;

            if (event.timer.tag.mutex_label.size())
            {
                auto& mutexAgg = mutexAggregates[event.timer.tag.mutexStr()];
                ++mutexAgg.first.count;
                mutexAgg.first.duration_us += event.timer.duration_us;
                auto& lockerAgg = mutexAgg.second[event.timer.tag.label];
                ++lockerAgg.count;
                lockerAgg.duration_us += event.timer.duration_us;

                continue; // mutex Tracers don't have sub-timers.
            }

            for (auto const& subTimer: event.sub_timers)
            {
                if (event.render)
                    traceJson.append(subTimer.toJson());

                auto& subAgg = agg.second[subTimer.tag];
                ++subAgg.count;
                subAgg.duration_us += subTimer.duration_us;

                if (subTimer.tag.mutex_label.size())
                {
                    auto& mutexAgg = mutexAggregates[subTimer.tag.mutexStr()];
                    ++mutexAgg.first.count;
                    mutexAgg.first.duration_us += subTimer.duration_us;
                    auto& lockerAgg = mutexAgg.second[subTimer.tag.label];
                    ++lockerAgg.count;
                    lockerAgg.duration_us += subTimer.duration_us;
                }
            }
            if (traceJson.size())
                eventsJson.append(traceJson);
        }
        if (eventsJson.size())
            tracesJson[jss::detail] = eventsJson;

        // put averages and percentages into aggregates
        for (auto& agg : aggregates)
        {
            double const totalDuration = static_cast<double>(
                agg.second.first.duration_us.count());
            assert(agg.second.first.count);
            agg.second.first.avg_us = agg.second.first.duration_us /
                agg.second.first.count;
            if (totalDuration) // this could happen with sub-ms timing
            {
                agg.second.first.percent =
                    static_cast<double>(
                        agg.second.first.duration_us.count() * 100) /
                    totalDuration;
            }
            else
            {
                agg.second.first.percent = 0;
            }

            for (auto& subAgg : agg.second.second)
            {
                assert(subAgg.second.count);
                 subAgg.second.avg_us = subAgg.second.duration_us /
                    subAgg.second.count;
                 if (totalDuration)
                 {
                     subAgg.second.percent =
                         static_cast<double>(
                             subAgg.second.duration_us.count() * 100) /
                         totalDuration;
                 }
                 else
                 {
                     subAgg.second.percent = 0;
                 }
            }
        }

        for (auto& mutexAgg : mutexAggregates)
        {
            double const totalDuration = static_cast<double>(
                mutexAgg.second.first.duration_us.count());
            assert(mutexAgg.second.first.count);
            mutexAgg.second.first.avg_us = mutexAgg.second.first.duration_us /
                                      mutexAgg.second.first.count;
            if (totalDuration)
            {
                mutexAgg.second.first.percent =
                    static_cast<double>(
                        mutexAgg.second.first.duration_us.count() * 100) /
                    totalDuration;
            }
            else
            {
                mutexAgg.second.first.percent = 0;
            }

            for (auto& lockAgg : mutexAgg.second.second)
            {
                lockAgg.second.avg_us = lockAgg.second.duration_us /
                                       lockAgg.second.count;
                if (totalDuration)
                {
                    lockAgg.second.percent =
                        static_cast<double>(
                            lockAgg.second.duration_us.count() * 100) /
                        totalDuration;
                }
                else
                {
                    lockAgg.second.percent = 0;
                }
            }
        }

        Json::Value aggregatesJson{Json::objectValue};
        Json::Value aggregateTracersJson{Json::objectValue};
        Json::Value aggregateMutexJson{Json::objectValue};

        std::map<Timers::Timer::Tag const,
            std::pair<Aggregate, std::multimap<double,
                std::pair<Timers::Timer::Tag, Aggregate>>>> aggregatesByPercent;
        for (auto const& a : aggregates)
        {
            Timers::Timer::Tag const& tag = a.first;
            std::multimap<
                double, std::pair<Timers::Timer::Tag, Aggregate>> subs;
            for (auto const& s : a.second.second)
                subs.insert({s.second.percent, s});
            aggregatesByPercent[tag] = {a.second.first, subs};
        }
        for (auto const& a : aggregatesByPercent)
        {
            Timers::Timer::Tag const& tag = a.first;
            aggregateTracersJson[std::string(tag.label)] = Json::objectValue;
            Json::Value& aggJsonRef =
                aggregateTracersJson[std::string(tag.label)];
            aggJsonRef[std::string(jss::stats)] = a.second.first.toJson();

            if (a.second.second.size())
            {
                aggJsonRef[jss::subtimers] = Json::arrayValue;
                Json::Value& subTimersRef = aggJsonRef[jss::subtimers];
                std::multimap<double, std::pair<Timers::Timer::Tag,
                    Aggregate>>::const_reverse_iterator rit;
                for (rit = a.second.second.rbegin();
                     rit != a.second.second.rend();
                     ++rit)
                {
                    Json::Value entry{Json::objectValue};
                    rit->second.first.toJson(entry);
                    entry[jss::stats] = rit->second.second.toJson();
                    subTimersRef.append(entry);
                }
            }
        }

        std::map<std::string,
            std::pair<Aggregate, std::multimap<double,
                std::pair<std::string_view, Aggregate>>>> mutexAggregatesByPercent;

        for (auto const& ma : mutexAggregates)
        {
            std::multimap<
                double, std::pair<std::string_view, Aggregate>> subs;
            for (auto const& l : ma.second.second)
                subs.insert({l.second.percent, l});
            mutexAggregatesByPercent[ma.first] =
                {ma.second.first, subs};
        }
        for (auto const& ma : mutexAggregatesByPercent)
        {
            std::string const& mutexStr = ma.first;
            aggregateMutexJson[mutexStr] = Json::objectValue;
            Json::Value& mutexAggJsonRef = aggregateMutexJson[mutexStr];
            mutexAggJsonRef[std::string(jss::stats)] = ma.second.first.toJson();

            if (ma.second.second.size())
            {
                mutexAggJsonRef[jss::locks] = Json::arrayValue;
                Json::Value& subTimersRef = mutexAggJsonRef[jss::locks];
                std::multimap<double, std::pair<std::string_view,
                    Aggregate>>::const_reverse_iterator rit;
                for (rit = ma.second.second.rbegin();
                     rit != ma.second.second.rend();
                     ++rit)
                {
                    Json::Value entry{Json::objectValue};
                    entry[jss::lock] = std::string(rit->second.first);
                    entry[jss::stats] = rit->second.second.toJson();
                    subTimersRef.append(entry);
                }
            }
        }

        aggregatesJson[jss::traces] = aggregateTracersJson;
        if (aggregateMutexJson.size())
            aggregatesJson[jss::mutex] = aggregateMutexJson;
        tracesJson[jss::aggregates] = aggregatesJson;
        report[jss::traces] = tracesJson;
    }
    */

    Json::Value tracesJson = reportEvents();
    if (tracesJson.size())
        report[jss::traces] = tracesJson;

    report[jss::report_duration_us] = std::to_string(
        std::chrono::duration_cast<microseconds>(
            std::chrono::steady_clock::now() - steadyPresent).count());

    logFile_ << Json::Compact{std::move(report)} << std::endl;
}

PerfLogImp::PerfLogImp(
    Setup const& setup,
    Stoppable& parent,
    beast::Journal journal,
    std::function<void()>&& signalStop,
    Application& app)
    : Stoppable("PerfLogImp", parent)
    , setup_(setup)
    , j_(journal)
    , signalStop_(std::move(signalStop))
    , app_(app)
{
    perfLog = this;
    openLog();
}

PerfLogImp::~PerfLogImp()
{
    report();
    onStop();
}

void
PerfLogImp::rpcStart(std::string const& method, std::uint64_t const requestId)
{
    auto counter = counters_.rpc_.find(method);
    if (counter == counters_.rpc_.end())
    {
        assert(false);
        return;
    }

    {
        std::lock_guard lock(counter->second.mutex);
        ++counter->second.value.started;
    }
    std::lock_guard lock(counters_.methodsMutex_);
    counters_.methods_[requestId] = {
        counter->first.c_str(), steady_clock::now()};
}

void
PerfLogImp::rpcEnd(
    std::string const& method,
    std::uint64_t const requestId,
    bool finish)
{
    auto counter = counters_.rpc_.find(method);
    if (counter == counters_.rpc_.end())
    {
        assert(false);
        return;
    }
    steady_time_point startTime;
    {
        std::lock_guard lock(counters_.methodsMutex_);
        auto const e = counters_.methods_.find(requestId);
        if (e != counters_.methods_.end())
        {
            startTime = e->second.second;
            counters_.methods_.erase(e);
        }
        else
        {
            assert(false);
        }
    }
    std::lock_guard lock(counter->second.mutex);
    if (finish)
        ++counter->second.value.finished;
    else
        ++counter->second.value.errored;
    counter->second.value.duration += std::chrono::duration_cast<microseconds>(
        steady_clock::now() - startTime);
}

void
PerfLogImp::jobQueue(JobType const type)
{
    auto counter = counters_.jq_.find(type);
    if (counter == counters_.jq_.end())
    {
        assert(false);
        return;
    }
    std::lock_guard lock(counter->second.mutex);
    ++counter->second.value.queued;
}

void
PerfLogImp::jobStart(
    JobType const type,
    microseconds dur,
    steady_time_point startTime,
    int instance)
{
    auto counter = counters_.jq_.find(type);
    if (counter == counters_.jq_.end())
    {
        assert(false);
        return;
    }
    {
        std::lock_guard lock(counter->second.mutex);
        ++counter->second.value.started;
        counter->second.value.queuedDuration += dur;
    }
    std::lock_guard lock(counters_.jobsMutex_);
    if (instance >= 0 && instance < counters_.jobs_.size())
        counters_.jobs_[instance] = {type, startTime};
}

void
PerfLogImp::jobFinish(JobType const type, microseconds dur, int instance)
{
    auto counter = counters_.jq_.find(type);
    if (counter == counters_.jq_.end())
    {
        assert(false);
        return;
    }
    {
        std::lock_guard lock(counter->second.mutex);
        ++counter->second.value.finished;
        counter->second.value.runningDuration += dur;
    }
    std::lock_guard lock(counters_.jobsMutex_);
    if (instance >= 0 && instance < counters_.jobs_.size())
        counters_.jobs_[instance] = {jtINVALID, steady_time_point()};
}

void
PerfLogImp::resizeJobs(int const resize)
{
    std::lock_guard lock(counters_.jobsMutex_);
    if (resize > counters_.jobs_.size())
        counters_.jobs_.resize(resize, {jtINVALID, steady_time_point()});
}

void
PerfLogImp::rotate()
{
    if (setup_.perfLog.empty())
        return;

    std::lock_guard lock(mutex_);
    rotate_ = true;
    cond_.notify_one();
}

void
PerfLogImp::onStart()
{
    if (setup_.perfLog.size())
        thread_ = std::thread(&PerfLogImp::run, this);
}

void
PerfLogImp::onStop()
{
    if (thread_.joinable())
    {
        {
            std::lock_guard lock(mutex_);
            stop_ = true;
            cond_.notify_one();
        }
        thread_.join();
    }
}

void
PerfLogImp::onChildrenStopped()
{
    stopped();
}

void
PerfLogImp::addEvent(Timers const& timers)
{
    std::lock_guard<std::mutex> lock(eventsMutex_);
    events_.push_back(timers);
}


//-----------------------------------------------------------------------------

PerfLog::Setup
setup_PerfLog(Section const& section, boost::filesystem::path const& configDir)
{
    PerfLog::Setup setup;
    std::string perfLog;
    set(perfLog, "perf_log", section);
    if (perfLog.size())
    {
        setup.perfLog = boost::filesystem::path(perfLog);
        if (setup.perfLog.is_relative())
        {
            setup.perfLog =
                boost::filesystem::absolute(setup.perfLog, configDir);
        }
    }

    std::uint64_t logInterval;
    if (get_if_exists(section, "log_interval", logInterval))
        setup.logInterval = std::chrono::seconds(logInterval);
    return setup;
}

std::unique_ptr<PerfLog>
make_PerfLog(
    PerfLog::Setup const& setup,
    Stoppable& parent,
    beast::Journal journal,
    std::function<void()>&& signalStop,
    Application* app)
{
    return std::make_unique<PerfLogImp>(
        setup, parent, journal, std::move(signalStop), *app);
}

}  // namespace perf
}  // namespace ripple
