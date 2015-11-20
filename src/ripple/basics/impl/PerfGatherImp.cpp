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

#include <ripple/basics/impl/PerfGatherImp.h>

namespace ripple {

PerfGatherImp::PerfGatherImp (Stoppable& parent,
        JobQueue& jobQueue,
        NetworkOPs& networkOps,
        NodeStore::Database& nodeStore,
        bool logging)
        : PerfGather (parent)
        , counters_ {jobQueue.getCounters(),
                networkOps.getTerCounters()}
        , jobQueue_ (jobQueue)
        , nodeStore_ (nodeStore)
        , logging_ (logging)
{}

std::array<std::uint64_t, 5> const PerfGatherImp::nodeStore()
{
    std::array<std::uint64_t, 5> ret;

    ret[0] = nodeStore_.getStoreCount();
    ret[1] = nodeStore_.getFetchTotalCount();
    ret[2] = nodeStore_.getFetchHitCount();
    ret[3] = nodeStore_.getStoreSize();
    ret[4] = nodeStore_.getFetchSize();

    return ret;
}

bool PerfGatherImp::isStopping()
{
    std::unique_lock<std::mutex> lock (mutex_);

    if (stopping_)
    {
        stopping_ = false;
        lock.unlock();
        cond_.notify_one();

        return true;
    }

    return false;
}

void PerfGatherImp::onStop()
{
    if (logging_)
    {
        std::unique_lock<std::mutex> lock (mutex_);
        stopping_ = true;
        cond_.wait (lock, [this]{return !stopping_;});
    }

    stopped();
}

//------------------------------------------------------------------------------

std::unique_ptr<PerfGather>
make_PerfGather (beast::Stoppable& parent,
        JobQueue& jobQueue,
        NetworkOPs& networkOps,
        NodeStore::Database& nodeStore,
        bool logging)
{
    return std::make_unique<PerfGatherImp> (parent,
            jobQueue,
            networkOps,
            nodeStore,
            logging);
}

} // ripple
