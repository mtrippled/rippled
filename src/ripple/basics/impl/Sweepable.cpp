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
#include <ripple/basics/Sweepable.h>
#include <ripple/core/JobQueue.h>

namespace ripple {

void
SweepQueue::replace(std::queue<std::pair<Sweepable*, char const*>>&& q)
{
    std::lock_guard<std::mutex> lock(mutex_);
    q_ = std::move(q);
    std::cerr << "sweep q size " << q_.size() << '\n';
}

void
SweepQueue::sweepOne()
{
    static JobQueue& jq = app_.getJobQueue();
    std::unique_lock<std::mutex> lock(mutex_);
    std::cerr << "sweep q size " << q_.size() << '\n';
    if (q_.empty())
        return;
    auto toSweep = q_.front();
    std::cerr << "sweep q job " << toSweep.second << '\n';
    if (jq.addJob(
        jtSWEEP,
        "sweepOne",
        [this, toSweep](Job&) {
            toSweep.first->sweep();
        }))
    {
        q_.pop();
    }
    std::cerr << "sweep q size " << q_.size() << '\n';
    if (q_.empty())
    {
        lock.unlock();
        app_.setSweepTimer();
    }
    std::cerr << "sweep q size " << q_.size() << '\n';
}


} // ripple