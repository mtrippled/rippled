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

#ifndef RIPPLE_LEDGER_CACHEDSLES_H_INCLUDED
#define RIPPLE_LEDGER_CACHEDSLES_H_INCLUDED

#include <ripple/basics/chrono.h>
#include <ripple/basics/Tracer.h>
#include <ripple/beast/container/aged_unordered_map.h>
#include <ripple/protocol/STLedgerEntry.h>
#include <boost/core/ignore_unused.hpp>
#include <memory>
#include <mutex>

namespace ripple {

/** Caches SLEs by their digest. */
class CachedSLEs
{
public:
    using digest_type = uint256;

    using value_type = std::shared_ptr<SLE const>;

    CachedSLEs(CachedSLEs const&) = delete;
    CachedSLEs&
    operator=(CachedSLEs const&) = delete;

    template <class Rep, class Period>
    CachedSLEs(
        std::chrono::duration<Rep, Period> const& timeToLive,
        Stopwatch& clock)
        : map_(clock)
    {
        boost::ignore_unused(timeToLive);
    }

    /** Discard expired entries.

        Needs to be called periodically.
    */
    void
    expire();

    /** Fetch an item from the cache.

        If the digest was not found, Handler
        will be called with this signature:

            std::shared_ptr<SLE const>(void)
    */
    template <class Handler>
    value_type
    fetch(digest_type const& digest, Handler const& h)
    {
        auto tracer = perf::TRACER;
        {
            std::lock_guard lock(mutex_);
            auto iter = writableMap_->find(digest);
            if (iter == writableMap_->end())
            {
                iter = archiveMap_->find(digest);
                if (iter != archiveMap_->end())
                {
                    ++hit_;
                    archiveMap_->touch(iter);
                    return iter->second;
                }
            }
            else
            {
                ++hit_;
                writableMap_->touch(iter);
                return iter->second;
            }
        }

        auto sle = h();
        if (!sle)
            return nullptr;
        std::lock_guard lock(mutex_);
        ++miss_;
        auto const [it, inserted] =
            writableMap_->emplace(digest, std::move(sle));
        if (!inserted)
            writableMap_->touch(it);
        return it->second;
    }

    /** Returns the fraction of cache hits. */
    double
    rate() const;

    void
    rotate()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::lock_guard<std::mutex> purgeLock(purgeMutex_);
        assert(!tmpMap_);
        tmpMap_.reset(archiveMap_.release());
        archiveMap_.reset(writableMap_.release());
        writableMap_ = std::make_unique<map_type>(stopwatch());
    }

    void
    purge()
    {
        std::lock_guard<std::mutex> lock(purgeMutex_);
        assert(tmpMap_);
        tmpMap_.reset();
    }

private:
    std::size_t hit_ = 0;
    std::size_t miss_ = 0;
    std::mutex mutable mutex_;
    std::mutex mutable purgeMutex_;
    //Stopwatch::duration timeToLive_;

    using map_type = beast::aged_unordered_map<
        digest_type,
        value_type,
        Stopwatch::clock_type,
        hardened_hash<strong_hash>>;

    beast::aged_unordered_map<
        digest_type,
        value_type,
        Stopwatch::clock_type,
        hardened_hash<strong_hash>>
        map_;

    std::unique_ptr<map_type> writableMap_{
        std::make_unique<map_type>(stopwatch())};
    std::unique_ptr<map_type> archiveMap_{
        std::make_unique<map_type>(stopwatch())};
    std::unique_ptr<map_type> tmpMap_{
        std::make_unique<map_type>(stopwatch())};
};

}  // namespace ripple

#endif
