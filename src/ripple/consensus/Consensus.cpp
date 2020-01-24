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
    std::chrono::milliseconds idleInterval,
    ConsensusParms const& parms,
    beast::Journal j,
    bool haveValidated)
{
    using namespace std::chrono_literals;
    JLOG(j.warn()) << "syncprofile shouldCloseLedger Trans="
                   << (anyTransactions ? "yes" : "no")
                   << " Prop: " << prevProposers << "/" << proposersClosed
                   << " Secs: " << timeSincePrevClose.count()
                   << " (last: " << prevRoundTime.count() << ")";
    if ((prevRoundTime < -1s) || (prevRoundTime > 10min) ||
        (timeSincePrevClose > 10min))
    {
        // These are unexpected cases, we just close the ledger
        JLOG(j.warn()) << "syncprofile shouldCloseLedger (true) "
                          "unexpected cases";
        return true;
    }

    if ((proposersClosed + proposersValidated) > (prevProposers / 2))
    {
        // If more than half of the network has closed, we close
        JLOG(j.debug()) << "syncprofile shouldCloseLedger (true) Others "
                           "have closed";
        return true;
    }

    if (!anyTransactions)
    {
        /*
        if (! haveValidated)
        {
            JLOG(j.debug()) << "syncprofile shouldCloseLedger (true) "
                               "initial startup";
            return true;
        }
         */
        // Only close at the end of the idle interval
        JLOG(j.debug()) << "syncprofile shouldCloseLedger ("
            << (timeSincePrevClose >= idleInterval ? "true" : "false")
            << ") only close at end of idle interval";
        return timeSincePrevClose >= idleInterval;  // normal idle
    }

    // Preserve minimum ledger open time
    if (openTime < parms.ledgerMIN_CLOSE)
    {
        JLOG(j.debug()) << "syncprofile shouldCloseLedger (false) Must wait"
                           "minimum time before closing";
        return false;
    }

    // Don't let this ledger close more than twice as fast as the previous
    // ledger reached consensus so that slower validators can slow down
    // the network
    if (openTime < (prevRoundTime / 2))
    {
        JLOG(j.debug()) << "syncprofile shouldCloseLedger (false) Ledger has "
                           "not been open long enough";
        return false;
    }

    // Close the ledger
    JLOG(j.debug()) << "syncprofile shouldCloseLedger (true) close ledger";
    return true;
}

bool
checkConsensusReached(
    std::size_t agreeing,
    std::size_t total,
    bool count_self,
    std::size_t minConsensusPct,
    beast::Journal j)
{
    // If we are alone, we have a consensus
    if (total == 0)
    {
        JLOG(j.debug()) << "syncprofile checkConsensusReached true total 0";
        return true;
    }

    if (count_self)
    {
        ++agreeing;
        ++total;
    }

    std::size_t currentPercentage = (agreeing * 100) / total;
    JLOG(j.debug()) << "syncprofile checkConsensusReached currentPercentage ("
        << currentPercentage
        << ") >= minConsensusPct (" << minConsensusPct
        << ") returning "
        << (currentPercentage >= minConsensusPct ? "true" : "false");

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
    JLOG(j.debug()) << "syncprofile checkConsensus: prop=" << currentProposers << "/"
                    << prevProposers << " agree=" << currentAgree
                    << " validated=" << currentFinished
                    << " time=" << currentAgreeTime.count() << "/"
                    << previousAgreeTime.count();

    if (currentAgreeTime <= parms.ledgerMIN_CONSENSUS)
    {
        JLOG(j.debug()) << "syncprofile checkConsensus not enough elapsed time";
        return ConsensusState::No;
    }

    if (currentProposers < (prevProposers * 3 / 4))
    {
        // Less than 3/4 of the last ledger's proposers are present; don't
        // rush: we may need more time.
        if (currentAgreeTime < (previousAgreeTime + parms.ledgerMIN_CONSENSUS))
        {
            JLOG(j.debug()) << "syncprofile checkConsensus too fast, not "
                               "enough proposers";
            return ConsensusState::No;
        }
    }

    // Have we, together with the nodes on our UNL list, reached the threshold
    // to declare consensus?
    if (checkConsensusReached(
            currentAgree, currentProposers, proposing, parms.minCONSENSUS_PCT,
            j))
    {
        JLOG(j.debug()) << "syncprofile checkConsensus normal consensus";
        return ConsensusState::Yes;
    }

    // Have sufficient nodes on our UNL list moved on and reached the threshold
    // to declare consensus?
    if (checkConsensusReached(
            currentFinished, currentProposers, false, parms.minCONSENSUS_PCT,
            j))
    {
        JLOG(j.warn()) << "syncprofile checkConsensus We see no consensus, "
                          "but 80% of nodes have moved on";
        return ConsensusState::MovedOn;
    }

    // no consensus yet
    JLOG(j.debug()) << "syncprofile checkConsensus no consensus";
    return ConsensusState::No;
}

}  // namespace ripple
