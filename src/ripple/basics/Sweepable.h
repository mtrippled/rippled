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

#ifndef RIPPLE_BASICS_SWEEPABLE_H
#define RIPPLE_BASICS_SWEEPABLE_H

#include <mutex>
#include <queue>
#include <utility>

namespace ripple {

class Sweepable
{
public:
    virtual void sweep() = 0;
    virtual ~Sweepable() = default;
};

//------------------------------------------------------------------------------

class Application;

class SweepQueue
{
    Application& app_;

    std::queue<std::pair<Sweepable*, char const*>> q_;
    std::mutex mutex_;

public:
    SweepQueue(Application& app)
        : app_(app)
    {}

    void
    replace(std::queue<std::pair<Sweepable*, char const*>>&& q);

    void
    sweepOne();
};

} // ripple

#endif  // RIPPLE_BASICS_SWEEPABLE_H
