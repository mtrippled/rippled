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

#ifndef RIPPLE_BASICS_IMPL_PERFGATHERIMP_H_INCLUDED
#define RIPPLE_BASICS_IMPL_PERFGATHERIMP_H_INCLUDED

#include <ripple/basics/PerfGather.h>
#include <ripple/basics/AtomicArray.h>
#include <ripple/core/JobQueue.h>
#include <ripple/app/misc/NetworkOPs.h>
#include <ripple/nodestore/Database.h>

namespace ripple {

class PerfGatherImp : public PerfGather
{
public:
    PerfGatherImp (Stoppable& parent,
        JobQueue& jobQueue,
        NetworkOPs& networkOps,
        NodeStore::Database& nodeStore,
        bool logging);

    Counters const& counters() override
    {
        return counters_;
    }

    std::array<std::uint64_t, 5> const nodeStore() override;

    int getNumberOfThreads() override
    {
        return jobQueue_.getNumberOfThreads();
    }

    bool isStopping() override;

    //
    // Stoppable
    //
    void onPrepare() override {}

    void onStart() override {}

    // Called when the application begins shutdown.
    void onStop() override;

    // Called when all child Stoppable objects have stopped.
    void onChildrenStopped() override {}

private:
    Counters counters_;
    JobQueue& jobQueue_;
    NodeStore::Database& nodeStore_;
    std::mutex mutex_;
    std::condition_variable cond_;
    bool stopping_ = false;
    bool logging_;
};

} // ripple

#endif
