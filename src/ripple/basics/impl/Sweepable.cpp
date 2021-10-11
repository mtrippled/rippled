//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2021 Ripple Labs Inc.

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
#include <ripple/basics/Log.h>
#include <ripple/basics/Sweepable.h>
#include <ripple/core/JobQueue.h>
#include <sstream>

namespace ripple {

SweepQueue::SweepQueue(Application& app)
    : app_(app)
    , j_(app.logs().journal("SweepQueue"))
{}

void
SweepQueue::replace(std::queue<std::pair<Sweepable*, char const*>>&& q)
{
    assert(q_.empty());
    std::size_t newSize = q.size();
    std::size_t before = q_.size();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        q_ = std::move(q);
    }

    std::stringstream ss;
    ss << "placed " << newSize << " jobs on queue";
    if (before)
    {
        JLOG(j_.error()) << ss.str() << ", replacing " << before << ". This is a problem because it "
                                                                    "means that some things were not swept.";;

    }
    else
    {
        JLOG(j_.info()) << ss.str();
    }
}

void
SweepQueue::sweepOne()
{
    static JobQueue& jq = app_.getJobQueue();
    bool res, empty;
    std::unique_lock<std::mutex> lock(mutex_);
    if (q_.empty())
        return;

    auto const toSweep = q_.front();
    res = jq.addJob(
        jtSWEEP,
        "sweepOne",
        [this, toSweep](Job&) {
            toSweep.first->sweep(j_);
        });
    if (res)
        q_.pop();

    empty = q_.empty();
    lock.unlock();

    std::stringstream ss;
    ss << "put " << toSweep.second << " on job queue.";
    if (res)
        JLOG(j_.debug()) << "successfully " << ss.str();
    else
        JLOG(j_.error()) << "failed to " << ss.str();

    if (empty)
        app_.setSweepTimer();
}

} // ripple
