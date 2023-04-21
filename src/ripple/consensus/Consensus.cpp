//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#include <ripple/basics/Log.h>
#include <ripple/consensus/Consensus.h>

namespace ripple {

bool
shouldCloseLedger(
    bool anyTransactions,
    std::size_t prevProposers,
    std::size_t proposersClosed,
    std::size_t proposersValidated,
    std::chrono::milliseconds prevRoundTime,
    std::chrono::milliseconds
        timeSincePrevClose,              // Time since last ledger's close time
    std::chrono::milliseconds openTime,  // Time waiting to close this ledger
    std::unique_ptr<std::chrono::milliseconds>& validationDelay,
    std::chrono::milliseconds idleInterval,
    ConsensusParms const& parms,
    beast::Journal j)
{
    using namespace std::chrono_literals;

    JLOG(j.debug()) << "consensuslog shouldCloseLedger parameters. "
                       "anyTransactions:" << anyTransactions <<
                       ",prevProposers:" << prevProposers <<
                       ",proposersClosed:" << proposersClosed <<
                       ",proposersValidated:" << proposersValidated <<
                       ",prevRoundTime:" << prevRoundTime.count() << "ms" <<
                       ",timeSincePrevClose:" << timeSincePrevClose.count() << "ms" <<
                       ",openTime:" << openTime.count() << "ms" <<
                       ",validationDelay:" << (validationDelay ? std::to_string(validationDelay->count()) : "none ") << "ms" <<
                       ",idleInterval:" << idleInterval.count() << "ms. "
                       "minimum ledger close time: " << parms.ledgerMIN_CLOSE.count() << "ms";

    if ((prevRoundTime < -1s) || (prevRoundTime > 10min) ||
        (timeSincePrevClose > 10min))
    {
        // These are unexpected cases, we just close the ledger
        JLOG(j.warn()) << "consensuslog closing based on unpexpected elapsed time. Trans=" << (anyTransactions ? "yes" : "no")
                       << " Prop: " << prevProposers << "/" << proposersClosed
                       << " Secs: " << timeSincePrevClose.count()
                       << " (last: " << prevRoundTime.count() << ")";
        return true;
    }

    if ((proposersClosed + proposersValidated) > (prevProposers / 2))
    {
        // If more than half of the network has closed, we close
        JLOG(j.debug()) << "consensuslog closing: Others have closed";
        return true;
    }

    // The openTime is the time spent so far waiting to close the ledger.
    // Any time spent retrying ledger validation in the previous round is
    // also counted.
    if (validationDelay)
        openTime += *validationDelay;

    if (!anyTransactions)
    {
        // Only close at the end of the idle interval
        bool const ret = timeSincePrevClose >= idleInterval;  // normal idle
        JLOG(j.debug()) << "consensuslog no transactions idle interval. closing? " <<
            (ret ? "passed" : "not yet passed");
        return ret;
    }

    // Preserve minimum ledger open time
    if (openTime < parms.ledgerMIN_CLOSE)
    {
        JLOG(j.debug()) << "consensuslog not closing, must wait minimum time";
        return false;
    }

    // Don't let this ledger close more than twice as fast as the previous
    // ledger reached consensus so that slower validators can slow down
    // the network
    if (openTime < (prevRoundTime / 2))
    {
        JLOG(j.debug()) << "consensuslog not closing, Ledger has not been open long enough";
        return false;
    }

    // Close the ledger
    JLOG(j.debug()) << "consensuslog closing: no reason no to";
    return true;
}

bool
checkConsensusReached(
    std::size_t agreeing,
    std::size_t total,
    bool count_self,
    std::size_t minConsensusPct)
{
    // If we are alone, we have a consensus
    if (total == 0)
        return true;

    if (count_self)
    {
        ++agreeing;
        ++total;
    }

    std::size_t currentPercentage = (agreeing * 100) / total;

    return currentPercentage >= minConsensusPct;
}

ConsensusState
checkConsensus(
    std::size_t prevProposers,
    std::size_t currentProposers,
    std::size_t currentAgree,
    std::size_t currentFinished,
    std::chrono::milliseconds previousAgreeTime,
    std::chrono::milliseconds currentAgreeTime,
    ConsensusParms const& parms,
    bool proposing,
    beast::Journal j)
{
    JLOG(j.debug()) << "consensuslog checkConsensus: prop=" << currentProposers << "/"
                    << prevProposers << " agree=" << currentAgree
                    << " validated=" << currentFinished
                    << " time=" << currentAgreeTime.count() << "/"
                    << previousAgreeTime.count()
                    << " proposing? " << proposing
                    << " minimum duration to reach consensus: "
                    << parms.ledgerMIN_CONSENSUS.count() << "ms"
                    << " minimum consensus percentage: " << parms.minCONSENSUS_PCT;

    if (currentProposers < (prevProposers * 3 / 4))
    {
        // Less than 3/4 of the last ledger's proposers are present; don't
        // rush: we may need more time.
        if (currentAgreeTime < (previousAgreeTime + parms.ledgerMIN_CONSENSUS))
        {
            JLOG(j.debug()) << "consensuslog too fast, not enough proposers, we don't have consensus";
            return ConsensusState::No;
        }
    }

    // Have we, together with the nodes on our UNL list, reached the threshold
    // to declare consensus?
    if (checkConsensusReached(
            currentAgree, currentProposers, proposing, parms.minCONSENSUS_PCT))
    {
        JLOG(j.debug()) << "consensuslog normal consensus reached";
        return ConsensusState::Yes;
    }

    // Have sufficient nodes on our UNL list moved on and reached the threshold
    // to declare consensus?
    if (checkConsensusReached(
            currentFinished, currentProposers, false, parms.minCONSENSUS_PCT))
    {
        JLOG(j.warn()) << "consensuslog We see no consensus, but 80% of nodes have moved on";
        return ConsensusState::MovedOn;
    }

    // no consensus yet
    JLOG(j.debug()) << "consensuslog checkConsensus no consensus";
    return ConsensusState::No;
}

}  // namespace ripple
