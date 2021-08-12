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

#ifndef RIPPLE_BASICS_TAGGEDCACHE_H_INCLUDED
#define RIPPLE_BASICS_TAGGEDCACHE_H_INCLUDED

#include <ripple/basics/Log.h>
#include <ripple/basics/UnorderedContainers.h>
#include <ripple/basics/hardened_hash.h>
#include <ripple/basics/partitioned_unordered_map.h>
#include <ripple/core/Job.h>
#include <ripple/core/JobQueue.h>
#include <ripple/beast/clock/abstract_clock.h>
#include <ripple/beast/insight/Insight.h>
#include <boost/asio.hpp>
#include <boost/asio/spawn.hpp>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

namespace ripple {

/** Map/cache combination.
    This class implements a cache and a map. The cache keeps objects alive
    in the map. The map allows multiple code paths that reference objects
    with the same tag to get the same actual object.

    So long as data is in the cache, it will stay in memory.
    If it stays in memory even after it is ejected from the cache,
    the map will track it.

    @note Callers must not modify data objects that are stored in the cache
          unless they hold their own lock over all cache operations.
*/
template <
    class Key,
    class T,
    class Hash = hardened_hash<>,
    class KeyEqual = std::equal_to<Key>,
    class Mutex = std::recursive_mutex>
class TaggedCache
{
public:
    using mutex_type = Mutex;
    using key_type = Key;
    using mapped_type = T;
    using clock_type = beast::abstract_clock<std::chrono::steady_clock>;

public:
    TaggedCache(
        std::string const& name,
        int size,
        clock_type::duration expiration,
        clock_type& clock,
        beast::Journal journal,
        beast::insight::Collector::ptr const& collector =
            beast::insight::NullCollector::New())
        : m_journal(journal)
        , m_clock(clock)
        , m_stats(
              name,
              std::bind(&TaggedCache::collect_metrics, this),
              collector)
        , m_name(name)
        , m_target_size(size)
        , m_target_age(expiration)
        , m_cache_count(0)
        , m_hits(0)
        , m_misses(0)
    {
    }

public:
    /** Return the clock associated with the cache. */
    clock_type&
    clock()
    {
        return m_clock;
    }

    int
    getTargetSize() const
    {
        std::lock_guard lock(m_mutex);
        return m_target_size;
    }

    void
    setTargetSize(int s)
    {
        std::lock_guard lock(m_mutex);
        m_target_size = s;

        if (s > 0)
            m_cache.rehash(static_cast<std::size_t>(
                (s + (s >> 2)) / m_cache.max_load_factor() + 1));

        JLOG(m_journal.debug()) << m_name << " target size set to " << s;
    }

    clock_type::duration
    getTargetAge() const
    {
        std::lock_guard lock(m_mutex);
        return m_target_age;
    }

    void
    setTargetAge(clock_type::duration s)
    {
        std::lock_guard lock(m_mutex);
        m_target_age = s;
        JLOG(m_journal.debug())
            << m_name << " target age set to " << m_target_age.count();
    }

    int
    getCacheSize() const
    {
        std::lock_guard lock(m_mutex);
        return m_cache_count;
    }

    int
    getTrackSize() const
    {
        std::lock_guard lock(m_mutex);
        return m_cache.size();
    }

    float
    getHitRate()
    {
        std::lock_guard lock(m_mutex);
        auto const total = static_cast<float>(m_hits + m_misses);
        return m_hits * (100.0f / std::max(1.0f, total));
    }

    void
    clear()
    {
        std::lock_guard lock(m_mutex);
        m_cache.clear();
        m_cache_count = 0;
    }

    void
    reset()
    {
        std::lock_guard lock(m_mutex);
        m_cache.clear();
        m_cache_count = 0;
        m_hits = 0;
        m_misses = 0;
    }

    void
    sweep()
    {
        int cacheRemovals = 0;
        int mapRemovals = 0;
        int cc = 0;

        // Keep references to all the stuff we sweep
        // so that we can destroy them outside the lock.
        //
        std::vector<std::shared_ptr<mapped_type>> stuffToSweep;

        {
            clock_type::time_point const now(m_clock.now());
            clock_type::time_point when_expire;

            std::lock_guard lock(m_mutex);

            if (m_target_size == 0 ||
                (static_cast<int>(m_cache.size()) <= m_target_size))
            {
                when_expire = now - m_target_age;
            }
            else
            {
                when_expire =
                    now - m_target_age * m_target_size / m_cache.size();

                clock_type::duration const minimumAge(std::chrono::seconds(1));
                if (when_expire > (now - minimumAge))
                    when_expire = now - minimumAge;

                JLOG(m_journal.trace())
                    << m_name << " is growing fast " << m_cache.size() << " of "
                    << m_target_size << " aging at "
                    << (now - when_expire).count() << " of "
                    << m_target_age.count();
            }

            stuffToSweep.reserve(m_cache.size());

            auto cit = m_cache.begin();

            while (cit != m_cache.end())
            {
                if (cit->second.isWeak())
                {
                    // weak
                    if (cit->second.isExpired())
                    {
                        ++mapRemovals;
                        cit = m_cache.erase(cit);
                    }
                    else
                    {
                        ++cit;
                    }
                }
                else if (cit->second.last_access <= when_expire)
                {
                    // strong, expired
                    --m_cache_count;
                    ++cacheRemovals;
                    if (cit->second.ptr.unique())
                    {
                        stuffToSweep.push_back(cit->second.ptr);
                        ++mapRemovals;
                        cit = m_cache.erase(cit);
                    }
                    else
                    {
                        // remains weakly cached
                        cit->second.ptr.reset();
                        ++cit;
                    }
                }
                else
                {
                    // strong, not expired
                    ++cc;
                    ++cit;
                }
            }
        }

        if (mapRemovals || cacheRemovals)
        {
            JLOG(m_journal.debug())
                << m_name << ": cache = " << m_cache.size() << "-"
                << cacheRemovals << ", map-=" << mapRemovals;
        }

        // At this point stuffToSweep will go out of scope outside the lock
        // and decrement the reference count on each strong pointer.
    }

    bool
    del(const key_type& key, bool valid)
    {
        // Remove from cache, if !valid, remove from map too. Returns true if
        // removed from cache
        std::lock_guard lock(m_mutex);

        auto cit = m_cache.find(key);

        if (cit == m_cache.end())
            return false;

        Entry& entry = cit->second;

        bool ret = false;

        if (entry.isCached())
        {
            --m_cache_count;
            entry.ptr.reset();
            ret = true;
        }

        if (!valid || entry.isExpired())
            m_cache.erase(cit);

        return ret;
    }

    /** Replace aliased objects with originals.

        Due to concurrency it is possible for two separate objects with
        the same content and referring to the same unique "thing" to exist.
        This routine eliminates the duplicate and performs a replacement
        on the callers shared pointer if needed.

        @param key The key corresponding to the object
        @param data A shared pointer to the data corresponding to the object.
        @param replace `true` if `data` is the up to date version of the object.

        @return `true` If the key already existed.
    */
private:
    template <bool replace>
    bool
    canonicalize(
        const key_type& key,
        std::conditional_t<
            replace,
            std::shared_ptr<T> const,
            std::shared_ptr<T>>& data)
    {
        // Return canonical value, store if needed, refresh in cache
        // Return values: true=we had the data already
        std::lock_guard lock(m_mutex);

        auto cit = m_cache.find(key);

        if (cit == m_cache.end())
        {
            m_cache.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(key),
                std::forward_as_tuple(m_clock.now(), data));
            ++m_cache_count;
            return false;
        }

        Entry& entry = cit->second;
        entry.touch(m_clock.now());

        if (entry.isCached())
        {
            if constexpr (replace)
            {
                entry.ptr = data;
                entry.weak_ptr = data;
            }
            else
            {
                data = entry.ptr;
            }

            return true;
        }

        auto cachedData = entry.lock();

        if (cachedData)
        {
            if constexpr (replace)
            {
                entry.ptr = data;
                entry.weak_ptr = data;
            }
            else
            {
                entry.ptr = cachedData;
                data = cachedData;
            }

            ++m_cache_count;
            return true;
        }

        entry.ptr = data;
        entry.weak_ptr = data;
        ++m_cache_count;

        return false;
    }

public:
    bool
    canonicalize_replace_cache(
        const key_type& key,
        std::shared_ptr<T> const& data)
    {
        return canonicalize<true>(key, data);
    }

    bool
    canonicalize_replace_client(const key_type& key, std::shared_ptr<T>& data)
    {
        return canonicalize<false>(key, data);
    }

    std::shared_ptr<T>
    fetch(const key_type& key)
    {
        // fetch us a shared pointer to the stored data object
        std::lock_guard lock(m_mutex);

        auto cit = m_cache.find(key);

        if (cit == m_cache.end())
        {
            ++m_misses;
            return {};
        }

        Entry& entry = cit->second;
        entry.touch(m_clock.now());

        if (entry.isCached())
        {
            ++m_hits;
            return entry.ptr;
        }

        entry.ptr = entry.lock();

        if (entry.isCached())
        {
            // independent of cache size, so not counted as a hit
            ++m_cache_count;
            return entry.ptr;
        }

        m_cache.erase(cit);
        ++m_misses;
        return {};
    }

    /** Insert the element into the container.
        If the key already exists, nothing happens.
        @return `true` If the element was inserted
    */
    bool
    insert(key_type const& key, T const& value)
    {
        auto p = std::make_shared<T>(std::cref(value));
        return canonicalize_replace_client(key, p);
    }

    // VFALCO NOTE It looks like this returns a copy of the data in
    //             the output parameter 'data'. This could be expensive.
    //             Perhaps it should work like standard containers, which
    //             simply return an iterator.
    //
    bool
    retrieve(const key_type& key, T& data)
    {
        // retrieve the value of the stored data
        auto entry = fetch(key);

        if (!entry)
            return false;

        data = *entry;
        return true;
    }

    /** Refresh the expiration time on a key.

        @param key The key to refresh.
        @return `true` if the key was found and the object is cached.
    */
    bool
    refreshIfPresent(const key_type& key)
    {
        bool found = false;

        // If present, make current in cache
        std::lock_guard lock(m_mutex);

        if (auto cit = m_cache.find(key); cit != m_cache.end())
        {
            Entry& entry = cit->second;

            if (!entry.isCached())
            {
                // Convert weak to strong.
                entry.ptr = entry.lock();

                if (entry.isCached())
                {
                    // We just put the object back in cache
                    ++m_cache_count;
                    entry.touch(m_clock.now());
                    found = true;
                }
                else
                {
                    // Couldn't get strong pointer,
                    // object fell out of the cache so remove the entry.
                    m_cache.erase(cit);
                }
            }
            else
            {
                // It's cached so update the timer
                entry.touch(m_clock.now());
                found = true;
            }
        }

        return found;
    }

    mutex_type&
    peekMutex()
    {
        return m_mutex;
    }

    std::vector<key_type>
    getKeys() const
    {
        std::vector<key_type> v;

        {
            std::lock_guard lock(m_mutex);
            v.reserve(m_cache.size());
            for (auto const& _ : m_cache)
                v.push_back(_.first);
        }

        return v;
    }

private:
    void
    collect_metrics()
    {
        m_stats.size.set(getCacheSize());

        {
            beast::insight::Gauge::value_type hit_rate(0);
            {
                std::lock_guard lock(m_mutex);
                auto const total(m_hits + m_misses);
                if (total != 0)
                    hit_rate = (m_hits * 100) / total;
            }
            m_stats.hit_rate.set(hit_rate);
        }
    }

private:
    struct Stats
    {
        template <class Handler>
        Stats(
            std::string const& prefix,
            Handler const& handler,
            beast::insight::Collector::ptr const& collector)
            : hook(collector->make_hook(handler))
            , size(collector->make_gauge(prefix, "size"))
            , hit_rate(collector->make_gauge(prefix, "hit_rate"))
        {
        }

        beast::insight::Hook hook;
        beast::insight::Gauge size;
        beast::insight::Gauge hit_rate;
    };

    class Entry
    {
    public:
        std::shared_ptr<mapped_type> ptr;
        std::weak_ptr<mapped_type> weak_ptr;
        clock_type::time_point last_access;

        Entry(
            clock_type::time_point const& last_access_,
            std::shared_ptr<mapped_type> const& ptr_)
            : ptr(ptr_), weak_ptr(ptr_), last_access(last_access_)
        {
        }

        bool
        isWeak() const
        {
            return ptr == nullptr;
        }
        bool
        isCached() const
        {
            return ptr != nullptr;
        }
        bool
        isExpired() const
        {
            return weak_ptr.expired();
        }
        std::shared_ptr<mapped_type>
        lock()
        {
            return weak_ptr.lock();
        }
        void
        touch(clock_type::time_point const& now)
        {
            last_access = now;
        }
    };

    using cache_type = hardened_hash_map<key_type, Entry, Hash, KeyEqual>;

    beast::Journal m_journal;
    clock_type& m_clock;
    Stats m_stats;

    mutex_type mutable m_mutex;

    // Used for logging
    std::string m_name;

    // Desired number of cache entries (0 = ignore)
    int m_target_size;

    // Desired maximum cache age
    clock_type::duration m_target_age;

    // Number of items cached
    int m_cache_count;
    cache_type m_cache;  // Hold strong reference to recent objects
    std::uint64_t m_hits;
    std::uint64_t m_misses;
};

//------------------------------------------------------------------------------

// Modified to trace mutex use.
template <
    class Key,
    class T,
    class Hash = hardened_hash<>,
    class KeyEqual = std::equal_to<Key>,
    class Mutex = std::recursive_mutex>
class TaggedCacheTrace
    : public std::enable_shared_from_this<TaggedCacheTrace<
        Key, T, Hash, KeyEqual, Mutex>>
{
public:
    using mutex_type = Mutex;
    using key_type = Key;
    using mapped_type = T;
    using clock_type = beast::abstract_clock<std::chrono::steady_clock>;

public:
    TaggedCacheTrace(
        std::string const& name,
        int size,
        clock_type::duration expiration,
        clock_type& clock,
        beast::Journal journal,
        std::function<std::uint64_t(Key const&)> extractor = [](Key const&) {
            return 0;},
        std::optional<std::size_t> partitions = std::nullopt,
        beast::insight::Collector::ptr const& collector =
        beast::insight::NullCollector::New())
        : m_journal(journal)
        , m_clock(clock)
        , m_stats(
            name,
            std::bind(&TaggedCacheTrace::collect_metrics, this),
            collector)
        , m_name(name)
        , m_target_size(size)
        , m_target_age(std::chrono::duration_cast<std::chrono::seconds>(expiration).count())
        , m_cache_count(0)
        , m_cache(extractor, partitions)
        , m_hits(0)
        , m_misses(0)
    {
        me_.reset(this);
    }

public:
    /** Return the clock associated with the cache. */
    clock_type&
    clock()
    {
        return m_clock;
    }

    int
    getTargetSize() const
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        return m_target_size;
    }

    void
    setTargetSize(int s)
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        m_target_size = s;

        if (s > 0)
        {
            for (auto& partition : m_cache.map())
            {
                std::lock_guard<std::mutex> lock(partition.mutex);
                partition.map.rehash(static_cast<std::size_t>(
                    (s + (s >> 2)) /
                    (partition.map.max_load_factor() * m_cache.partitions())
                    + 1));
            }
        }

        JLOG(m_journal.debug()) << m_name << " target size set to " << s;
    }

    clock_type::duration
    getTargetAge() const
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        return std::chrono::seconds(m_target_age);
    }

    void
    setTargetAge(clock_type::duration s)
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        m_target_age = std::chrono::duration_cast<std::chrono::seconds>(s).count();
        JLOG(m_journal.debug())
            << m_name << " target age set to " << m_target_age;
    }

    int
    getCacheSize() const
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        return m_cache_count;
    }

    int
    getTrackSize() const
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        return m_cache.size();
    }

    float
    getHitRate()
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        auto const total = static_cast<float>(m_hits + m_misses);
        return m_hits * (100.0f / std::max(1.0f, total));
    }

    void
    clear()
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        m_cache.clear();
        m_cache_count = 0;
    }

    void
    reset()
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        m_cache.clear();
        m_cache_count = 0;
        m_hits = 0;
        m_misses = 0;
    }

    void
    sweep(boost::asio::io_service& io)
    {
        {
            clock_type::time_point const now(m_clock.now());
            clock_type::time_point when_expire;

            auto tracer = perf::TRACER_PTR;
//            perf::LOCK_GUARD_TRACER(m_mutex, tracer, lock);

            std::size_t const cacheSize = m_cache.size();
            auto timer = perf::START_TIMER(tracer);
            std::chrono::seconds const age(m_target_age);
            if (m_target_size == 0 ||
                (static_cast<int>(cacheSize) <= m_target_size))
            {
                when_expire = now - age;
            }
            else
            {
                when_expire =
                    now - age * m_target_size.load() / cacheSize;

                clock_type::duration const minimumAge(std::chrono::seconds(1));
                if (when_expire > (now - minimumAge))
                    when_expire = now - minimumAge;

                JLOG(m_journal.trace())
                    << m_name << " is growing fast " << m_cache.size() << " of "
                    << m_target_size << " aging at "
                    << (now - when_expire).count() << " of "
                    << m_target_age;
            }
            perf::END_TIMER(tracer, timer);

            std::mutex mtx;
            std::condition_variable cond;
            std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
            std::size_t remaining = m_cache.partitions();

            auto self = this->shared_from_this();
            for (auto& partition : m_cache.map())
            {
                boost::asio::spawn(
                    io, [&, self](boost::asio::yield_context yield) {
                  int cacheRemovals = 0;
                  int mapRemovals = 0;
                  int cc = 0;

                  // Keep references to all the stuff we sweep
                  // so that we can destroy them outside the lock.
                  std::vector<std::shared_ptr<mapped_type>> stuffToSweep;
                  {
                      std::lock_guard<std::mutex> partitionLock(
                          partition.mutex);
                      stuffToSweep.reserve(partition.map.size());
                      auto cit = partition.map.begin();
                      while (cit != partition.map.end())
                      {
                          if (cit->second.isWeak())
                          {
                              // weak
                              if (cit->second.isExpired())
                              {
                                  ++mapRemovals;
                                  cit = partition.map.erase(cit);
                              }
                              else
                              {
                                  ++cit;
                              }
                          }
                          else if (cit->second.last_access <= when_expire)
                          {
                              // strong, expired
                              --m_cache_count;
                              ++cacheRemovals;
                              if (cit->second.ptr.unique())
                              {
                                  stuffToSweep.push_back(cit->second.ptr);
                                  ++mapRemovals;
                                  cit = partition.map.erase(cit);
                              }
                              else
                              {
                                  // remains weakly cached
                                  cit->second.ptr.reset();
                                  ++cit;
                              }
                          }
                          else
                          {
                              // strong, not expired
                              ++cc;
                              ++cit;
                          }
                      }
                  }

                  if (mapRemovals || cacheRemovals)
                  {
                      JLOG(m_journal.debug())
                          << "tncache sweep " << m_name
                          << ": cache = " << partition.map.size() << "-"
                          << cacheRemovals << ", map-=" << mapRemovals;
                  }

                  lock.lock();
                  --remaining;
                  m_cache_count -= cacheRemovals;
                  lock.unlock();
                  cond.notify_one();

                  // At this point stuffToSweep will go out of scope outside the lock and decrement the reference count on each strong pointer.
                });
            }

            lock.lock();
            cond.wait(lock, [&remaining]{
              return remaining == 0; });
        }
    }

    void
    sweep() {}

//    void
//    sweep()
//    {
//        JLOG(m_journal.debug()) << "tncache sweep1";
//        /*
//        {
//            typename partitioned_cache_type::iterator it;
//            typename partitioned_cache_type::const_iterator cit;
//            ++it;
//            ++cit;
//            it++;
//            cit++;
//            typename partitioned_cache_type::iterator it2;
//            typename partitioned_cache_type::const_iterator cit2;
//            if (it == it2)
//            {}
//            if (it != it2)
//            {}
//            if (cit == cit2)
//            {}
//            if (cit != cit2)
//            {}
//            if (it == cit)
//            {}
//            if (it != cit)
//            {}
//            if (cit == it)
//            {}
//            if (cit != it)
//            {}
//
//            it = it2;
//            cit = cit2;
//            typename partitioned_cache_type::iterator it3(it);
//            it3 = it;
//            typename partitioned_cache_type::const_iterator cit3(cit);
//
//            typename partitioned_cache_type::const_iterator cit4(it);
//            cit3 = it;
//            // shouldn't work (const to non-const)
////        typename partitioned_cache_type::iterator it4(cit);
////        it = cit;
//
//            typename partitioned_cache_type::map_type::iterator mit;
//            typename partitioned_cache_type::map_type::iterator mit2;
//            if (mit == mit2) {}
//            typename partitioned_cache_type::map_type::const_iterator cmit;
//            typename partitioned_cache_type::map_type::const_iterator cmit2;
//            if (cmit == cmit2) {}
//
//            it = partitioned_cache_.begin();
//            it = partitioned_cache_.end();
//            cit = partitioned_cache_.cbegin();
//            cit = partitioned_cache_.cend();
//        }
//         */
//
//        int cacheRemovals = 0;
//        int mapRemovals = 0;
//        int cc = 0;
//
//        // Keep references to all the stuff we sweep
//        // so that we can destroy them outside the lock.
//        //
//        std::vector<std::shared_ptr<mapped_type>> stuffToSweep;
//
////        cache_type tmpCache;
//        {
//            clock_type::time_point const now(m_clock.now());
//            clock_type::time_point when_expire;
//
//            auto tracer = perf::TRACER_PTR;
//            perf::LOCK_GUARD_TRACER(m_mutex, tracer, lock);
//
//// The result of the following is about 0.5us per item in cache.
////            {
////                auto const b4 = std::chrono::steady_clock::now();
////                tmpCache = m_cache;
////                JLOG(m_journal.debug()) << "copied cache size: "
////                    << tmpCache.size() << "in "
////                    << std::chrono::duration_cast<std::chrono::microseconds>(
////                        std::chrono::steady_clock::now() - b4).count()
////                    << "us";
////            }
//
//            auto timer = perf::START_TIMER(tracer);
//            if (m_target_size == 0 ||
//                (static_cast<int>(m_cache.size()) <= m_target_size))
//            {
//                when_expire = now - m_target_age;
//            }
//            else
//            {
//                when_expire =
//                    now - m_target_age * m_target_size / m_cache.size();
//
//                clock_type::duration const minimumAge(std::chrono::seconds(1));
//                if (when_expire > (now - minimumAge))
//                    when_expire = now - minimumAge;
//
//                JLOG(m_journal.trace())
//                    << m_name << " is growing fast " << m_cache.size() << " of "
//                    << m_target_size << " aging at "
//                    << (now - when_expire).count() << " of "
//                    << m_target_age.count();
//            }
//            perf::END_TIMER(tracer, timer);
//
//            auto timer2 = perf::START_TIMER(tracer);
//            stuffToSweep.reserve(m_cache.size());
//
//            JLOG(m_journal.debug()) << "tncache sweep10";
//            auto cit = m_cache.begin();
//
//            while (cit != m_cache.end())
//            {
//                if (cit->second.isWeak())
//                {
//                    // weak
//                    if (cit->second.isExpired())
//                    {
//                        ++mapRemovals;
//                        cit = m_cache.erase(cit);
//                    }
//                    else
//                    {
//                        ++cit;
//                    }
//                }
//                else if (cit->second.last_access <= when_expire)
//                {
//                    // strong, expired
//                    --m_cache_count;
//                    ++cacheRemovals;
//                    if (cit->second.ptr.unique())
//                    {
//                        stuffToSweep.push_back(cit->second.ptr);
//                        ++mapRemovals;
//                        cit = m_cache.erase(cit);
//                    }
//                    else
//                    {
//                        // remains weakly cached
//                        cit->second.ptr.reset();
//                        ++cit;
//                    }
//                }
//                else
//                {
//                    // strong, not expired
//                    ++cc;
//                    ++cit;
//                }
//            }
//            perf::END_TIMER(tracer, timer2);
//        }
//
//        JLOG(m_journal.debug()) << "tncache sweep30";
//        if (mapRemovals || cacheRemovals)
//        {
//            JLOG(m_journal.debug()) << "tncache sweep "
//                << m_name << ": cache = " << m_cache.size() << "-"
//                << cacheRemovals << ", map-=" << mapRemovals;
//        }
//
//        // At this point stuffToSweep will go out of scope outside the lock
//        // and decrement the reference count on each strong pointer.
//    }

    bool
    del(const key_type& key, bool valid)
    {
        // Remove from cache, if !valid, remove from map too. Returns true if
        // removed from cache
//        perf::LOCK_GUARD(m_mutex, lock);

        bool ret = false;
        m_cache.lockPartitionExec(key,
            [&key, &valid, &ret, this]
            (typename partitioned_cache_type::map_type& map) {
            auto cit = map.find(key);

            if (cit == map.end())
                return;

            Entry& entry = cit->second;

            //        bool ret = false;

            if (entry.isCached())
            {
                --m_cache_count;
                entry.ptr.reset();
                ret = true;
            }

            if (!valid || entry.isExpired())
                map.erase(cit);
        });

        return ret;
    }

    /** Replace aliased objects with originals.

        Due to concurrency it is possible for two separate objects with
        the same content and referring to the same unique "thing" to exist.
        This routine eliminates the duplicate and performs a replacement
        on the callers shared pointer if needed.

        @param key The key corresponding to the object
        @param data A shared pointer to the data corresponding to the object.
        @param replace `true` if `data` is the up to date version of the object.

        @return `true` If the key already existed.
    */
private:
    template <bool replace>
    bool
    canonicalize(
        const key_type& key,
        std::conditional_t<
            replace,
            std::shared_ptr<T> const,
            std::shared_ptr<T>>& data,
            std::shared_ptr<perf::Tracer> const& tracer = {})
    {
        // Return canonical value, store if needed, refresh in cache
        // Return values: true=we had the data already
//        perf::LOCK_GUARD_TRACER(m_mutex, tracer, lock);
        auto label = perf::START_TIMER(tracer);

        auto label2 = perf::START_TIMER(tracer);
        bool ret = false;
        std::lock_guard<std::mutex> lock(m_cache.partitionMutex(key));
        m_cache.partitionExec(key,
            [&, this]
            (typename partitioned_cache_type::map_type& map) {
            auto cit = map.find(key);
            perf::END_TIMER(tracer, label2);

            if (cit == map.end())
            {
                auto label3 = perf::START_TIMER(tracer);
                map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(key),
                    std::forward_as_tuple(m_clock.now(), data));
                ++m_cache_count;
                perf::END_TIMER(tracer, label3);
                perf::END_TIMER(tracer, label);
                return;
            }

            Entry& entry = cit->second;
            entry.touch(m_clock.now());

            if (entry.isCached())
            {
                if constexpr (replace)
                {
                    entry.ptr = data;
                    entry.weak_ptr = data;
                }
                else
                {
                    data = entry.ptr;
                }

                perf::END_TIMER(tracer, label);
                ret = true;
                return;
            }
            perf::END_TIMER(tracer, label);
            label = perf::START_TIMER(tracer);

            auto cachedData = entry.lock();

            if (cachedData)
            {
                if constexpr (replace)
                {
                    entry.ptr = data;
                    entry.weak_ptr = data;
                }
                else
                {
                    entry.ptr = cachedData;
                    data = cachedData;
                }

                ++m_cache_count;
                perf::END_TIMER(tracer, label);
                ret = true;
                return;
            }

            entry.ptr = data;
            entry.weak_ptr = data;
            ++m_cache_count;
        });

        perf::END_TIMER(tracer, label);
        return ret;
//        return false;
    }

public:
    bool
    canonicalize_replace_cache(
        const key_type& key,
        std::shared_ptr<T> const& data)
    {
        return canonicalize<true>(key, data);
    }

    bool
    canonicalize_replace_client(const key_type& key, std::shared_ptr<T>& data,
        std::shared_ptr<perf::Tracer> const& tracer = {})
    {
        return canonicalize<false>(key, data, tracer);
    }

    std::shared_ptr<T>
    fetch(const key_type& key)
    {
        // fetch us a shared pointer to the stored data object
//        perf::LOCK_GUARD(m_mutex, lock);

        typename partitioned_cache_type::map_type::iterator cit;
        bool found = false;
        std::lock_guard<std::mutex> lock(m_cache.partitionMutex(key));
        m_cache.partitionExec(key,
            [&key, &cit, &found, this]
            (typename partitioned_cache_type::map_type& map) {
                    cit = map.find(key);
                    if (cit == map.end())
                        ++m_misses;
                    else
                        found = true;
        });
        if (!found)
            return {};

        Entry& entry = cit->second;
        entry.touch(m_clock.now());
        if (entry.isCached())
        {
            ++m_hits;
            return entry.ptr;
        }
        entry.ptr = entry.lock();
        if (entry.isCached())
        {
            // independent of cache size, so not counted as a hit
            ++m_cache_count;
            return entry.ptr;
        }

        m_cache.partitionExec(key,
            [&cit]
            (typename partitioned_cache_type::map_type& map) {
               map.erase(cit);
        });
        ++m_misses;
        return {};
    }

    /** Fetch an item from the cache.

        If the digest was not found, Handler
        will be called with this signature:

            std::shared_ptr<SLE const>(void)
    */
    template <class Handler>
    std::shared_ptr<T>
    fetch(key_type const& digest, Handler const& h)
    {
        {
            typename partitioned_cache_type::map_type::iterator iter;
            bool found;
            std::unique_lock<std::mutex> lock(m_cache.partitionMutex(digest));
            m_cache.partitionExec(
                digest,
                [&digest, &iter, &found]
                (typename partitioned_cache_type::map_type& map) {
                    iter = map.find(digest);
                    found = iter != map.end();
                });

            if (found)
            {
                ++m_hits;
                Entry& entry = iter->second;
                entry.touch(m_clock.now());
                if (entry.isCached())
                {
                    ++m_hits;
                    return entry.ptr;
                }

                entry.ptr = entry.lock();
                if (entry.isCached())
                {
                    // independent of cache size, so not counted as a
                    // hit
                    ++m_cache_count;
                    return entry.ptr;
                }

                m_cache.partitionExec(
                    digest,
                    [&iter](
                        typename partitioned_cache_type::map_type& map) {
                            map.erase(iter);
                    });
            }
        }

        auto sle = h();
        if (!sle)
            return {};
//        perf::LOCK_GUARD(m_mutex, lock);
        ++m_misses;

        typename partitioned_cache_type::map_type::iterator it;
        m_cache.lockPartitionExec(
            digest,
            [&digest, &it, &sle, this]
            (typename partitioned_cache_type::map_type& map) {
                bool inserted;
                std::tie(it, inserted) = map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(digest),
                    std::forward_as_tuple(m_clock.now(), sle));
                if (!inserted)
                    it->second.touch(m_clock.now());
        });

        return it->second.ptr;
    }

    /** Insert the element into the container.
        If the key already exists, nothing happens.
        @return `true` If the element was inserted
    */
    bool
    insert(key_type const& key, T const& value)
    {
        auto p = std::make_shared<T>(std::cref(value));
        return canonicalize_replace_client(key, p);
    }

    // VFALCO NOTE It looks like this returns a copy of the data in
    //             the output parameter 'data'. This could be expensive.
    //             Perhaps it should work like standard containers, which
    //             simply return an iterator.
    //
    bool
    retrieve(const key_type& key, T& data)
    {
        // retrieve the value of the stored data
        auto entry = fetch(key);

        if (!entry)
            return false;

        data = *entry;
        return true;
    }

    /** Refresh the expiration time on a key.

        @param key The key to refresh.
        @return `true` if the key was found and the object is cached.
    */
    /*
    bool
    refreshIfPresent(const key_type& key)
    {
        bool found = false;

        // If present, make current in cache
        perf::LOCK_GUARD(m_mutex, lock);

        if (auto cit = m_cache.find(key); cit != m_cache.end())
        {
            Entry& entry = cit->second;

            if (!entry.isCached())
            {
                // Convert weak to strong.
                entry.ptr = entry.lock();

                if (entry.isCached())
                {
                    // We just put the object back in cache
                    ++m_cache_count;
                    entry.touch(m_clock.now());
                    found = true;
                }
                else
                {
                    // Couldn't get strong pointer,
                    // object fell out of the cache so remove the entry.
                    m_cache.erase(cit);
                }
            }
            else
            {
                // It's cached so update the timer
                entry.touch(m_clock.now());
                found = true;
            }
        }

        return found;
    }
     */

//    mutex_type&
    perf::mutex<mutex_type>&
    peekMutex()
    {
        return m_mutex2;
    }

    std::vector<key_type>
    getKeys() const
    {
        std::vector<key_type> v;

        {
//            perf::LOCK_GUARD(m_mutex, lock);
            v.reserve(m_cache.size());
            for (auto const& _ : m_cache)
                v.push_back(_.first);
        }

        return v;
    }

    double
    rate() const
    {
//        perf::LOCK_GUARD(m_mutex, lock);
        auto const tot = m_hits + m_misses;
        if (tot == 0)
            return 0;
        return double(m_hits) / tot;
    }

    void
    expire()
    {
        sweep();
    }

private:
    void
    collect_metrics()
    {
        m_stats.size.set(getCacheSize());

        {
            beast::insight::Gauge::value_type hit_rate(0);
            {
//                perf::LOCK_GUARD(m_mutex, lock);
                auto const total(m_hits + m_misses);
                if (total != 0)
                    hit_rate = (m_hits * 100) / total;
            }
            m_stats.hit_rate.set(hit_rate);
        }
    }

private:
    struct Stats
    {
        template <class Handler>
        Stats(
            std::string const& prefix,
            Handler const& handler,
            beast::insight::Collector::ptr const& collector)
            : hook(collector->make_hook(handler))
            , size(collector->make_gauge(prefix, "size"))
            , hit_rate(collector->make_gauge(prefix, "hit_rate"))
        {
        }

        beast::insight::Hook hook;
        beast::insight::Gauge size;
        beast::insight::Gauge hit_rate;
    };

    class Entry
    {
    public:
        std::shared_ptr<mapped_type> ptr;
        std::weak_ptr<mapped_type> weak_ptr;
        clock_type::time_point last_access;

        Entry(
            clock_type::time_point const& last_access_,
            std::shared_ptr<mapped_type> const& ptr_)
            : ptr(ptr_), weak_ptr(ptr_), last_access(last_access_)
        {
        }

        bool
        isWeak() const
        {
            return ptr == nullptr;
        }
        bool
        isCached() const
        {
            return ptr != nullptr;
        }
        bool
        isExpired() const
        {
            return weak_ptr.expired();
        }

        std::shared_ptr<mapped_type>
        lock()
        {
            return weak_ptr.lock();
        }
        void
        touch(clock_type::time_point const& now)
        {
            last_access = now;
        }
    };

//    using cache_type = hardened_hash_map<key_type, Entry, Hash, KeyEqual>;
    using partitioned_cache_type = partitioned_unordered_map<key_type,
        Entry, Hash, KeyEqual>;

    beast::Journal m_journal;
    clock_type& m_clock;
    Stats m_stats;

//    mutex_type mutable m_mutex;
    perf::mutex<mutex_type> mutable m_mutex2{FILE_LINE};

    // Used for logging
    std::string m_name;

    // Desired number of cache entries (0 = ignore)
    std::atomic<int> m_target_size;

    // Desired maximum cache age
    std::atomic<std::int64_t> m_target_age;
//    clock_type::duration m_target_age;

    // Number of items cached
    std::atomic<int> m_cache_count;
//    cache_type m_cache;  // Hold strong reference to recent objects
    partitioned_cache_type m_cache;
//    partitioned_cache_type m_cache{[](Key const& key) {
//      return *reinterpret_cast<std::uint64_t const*>(key.data());}};
    std::atomic<std::uint64_t> m_hits;
    std::atomic<std::uint64_t> m_misses;

    std::shared_ptr<TaggedCacheTrace<Key, T, Hash, KeyEqual, Mutex>> me_;
};

}  // namespace ripple

#endif
