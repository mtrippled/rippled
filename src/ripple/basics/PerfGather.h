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

#ifndef RIPPLE_BASICS_PERFGATHER_H_INCLUDED
#define RIPPLE_BASICS_PERFGATHER_H_INCLUDED

#include <beast/threads/Stoppable.h>

namespace ripple {

class JobQueue;
class NetworkOPs;
class PerfLog;

namespace NodeStore { class Database; }

/**
 * This class gathers performance data from instances of other classes. It
 * does this on behalf of PerfLog, and is only accessible by PerfLog. This
 * avoids cyclic dependencies.
 */
class PerfGather
    : public beast::Stoppable
{
public:
    /**
     * References to counters in singletons.
     */
    struct Counters
    {
        AtomicArray<std::uint64_t>& jobQueueCounters;
        AtomicArray<std::uint64_t>& terCounters;
    };

    PerfGather (Stoppable& parent)
        : Stoppable ("PerfGather", parent)
    {}

    /**
     * Get references to counters in singletons.
     *
     * @return References to counters in singletons.
     */
    virtual Counters const& counters() = 0;

    /**
     * Get nodestore fetch and store counters.
     *
     * @return Nodestore fetch and store counters.
     */
    virtual std::array<std::uint64_t, 5> const nodeStore() = 0;

    /** Get number of worker threads from JobQueue. */
    virtual int getNumberOfThreads() = 0;

    /** Check if we're trying to stop, and let us. */
    virtual bool isStopping() = 0;
};

//------------------------------------------------------------------------------

std::unique_ptr<PerfGather>
make_PerfGather (beast::Stoppable& parent,
        JobQueue& jobQueue,
        NetworkOPs& networkOps,
        NodeStore::Database& nodeStore,
        bool logging);

} // ripple

#endif
