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

#ifndef RIPPLE_BASICS_IMPL_PERFLOGIMP_H_INCLUDED
#define RIPPLE_BASICS_IMPL_PERFLOGIMP_H_INCLUDED

#include <ripple/basics/PerfLog.h>
#include <ripple/basics/PerfGather.h>
#include <ripple/app/main/Application.h>
#include <beast/module/core/system/SystemStats.h>
#if BEAST_LINUX || BEAST_MAC || BEAST_BSD
#include <unistd.h>
#include <sys/types.h>
#endif

namespace ripple {

class PerfLogImp : public PerfLog
{
public:
    PerfLogImp (std::string const& perf_log,
            unsigned int const log_interval,
            Stoppable& parent);

    ~PerfLogImp() override;

    void rotate() override;

private:
    std::string perf_log_;
    unsigned int log_interval_;
    bool rotate_ = false;
    PerfGather* perfGather_ = nullptr;
    bool stop_ = false;
    std::ofstream perfLog_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
    std::chrono::time_point<std::chrono::system_clock> lastLog_;
    std::string const cHostname_ = beast::getComputerName();
#if BEAST_LINUX || BEAST_MAC || BEAST_BSD
    unsigned int cPid_ = getpid();
#else
    unsigned int cPid_ = 0;
#endif
    std::multimap<std::chrono::time_point<std::chrono::system_clock>,
        PerfEvents> perfEvents_;
    std::mutex eventsMutex_;

    void openLog()
    {
        if (perf_log_.size())
            perfLog_.open (perf_log_, std::ios::out | std::ios::app);
    }

    void reOpenLog();
    void run();

    void startGather (PerfGather* perfGather) override
    {
        perfGather_ = perfGather;
    }

    void reportHeader (
        std::chrono::time_point<std::chrono::system_clock> const& now,
        Json::Value& report);

    std::string timeString (
        std::chrono::time_point<std::chrono::system_clock> const& tp);

    void report();

    void addEvent (PerfEvents const& event) override;

    bool logging() override
    {
        return perf_log_.size() != 0;
    }

    //
    // Stoppable
    //
    void onPrepare() override {}

    void onStart() override;

    // Called when the application begins shutdown.
    void onStop() override;

    // Called when all child Stoppable objects have stopped.
    void onChildrenStopped() override {}
};

} // ripple

#endif
