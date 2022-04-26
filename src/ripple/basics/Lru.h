//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2022 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_LRU_H
#define RIPPLE_BASICS_LRU_H

#include <ripple/basics/hardened_hash.h>
#include <ripple/basics/partitioned_unordered_map.h>
#include <ripple/beast/hash/hash_append.h>
#include <ripple/beast/hash/uhash.h>
#include <ripple/beast/hash/xxhasher.h>
#include <boost/circular_buffer.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <iostream>

namespace ripple {

template <
    typename Key,
    typename Value,
    typename Hash = hardened_hash<beast::xxhasher>,
    typename Pred = std::equal_to<Key>>
class Lru
{
public:
    using Entry = std::pair<Key, std::shared_ptr<Value>>;
    using q_type = boost::circular_buffer<Entry>;
//    using q_type = std::list<Entry>;
//    using map_type = std::unordered_map<Key, Value, Hash, Pred>;
    using map_type = std::unordered_map<Key,
          std::pair<typename q_type::iterator, std::size_t>,
                                        Hash, Pred>;

private:
    struct Partition
    {
        std::size_t capacity;
        q_type q;
        map_type map;
        std::mutex mtx;
        std::size_t evicted {0};

        Partition(std::size_t const cap)
            : capacity(cap)
            , q(q_type(cap))
        {
            map.reserve(cap);
        }

        Partition(Partition const& orig)
        {
            for (auto const& e : orig.q)
                q.push_back(e);
            map = orig.map;
            capacity = orig.capacity;
            evicted = orig.evicted;
        }

        typename q_type::iterator
        enqueue(Key const& key, std::shared_ptr<Value> const& value)
        {
            if (q.size() == capacity)
            {
                auto found = map.find(q.back().first);
                if (found != map.end() && --found->second.second == 0)
                    map.erase(found);
                ++evicted;
            }
            q.push_front({key, value});
            return q.begin();
        }

        void
        evict()
        {
            if (q.size() != capacity)
                return;
            auto found = map.find(q.back().first);
            if (found == map.end()) // could have been deleted
                return;
            if (--found->second.second == 0) // delete if no more accesses
                map.erase(found);

//            while (map.size() > capacity)
//            {
//                assert(q.size());
//                auto last = q.back();
//                map.erase(last.first);
//                q.pop_back();
//                ++evicted;
//            }
        }
    };

public:
    Lru(std::size_t const capacity,
        std::optional<std::size_t> partitions = std::nullopt)
    {
        // Set partitions to the number of hardware threads if the parameter
        // is either empty or set to 0.
        partitions_ = partitions && *partitions
                      ? *partitions
                      : std::thread::hardware_concurrency();
        assert(partitions_);
        cache_.reserve(partitions_);
        std::size_t const psize = capacity / partitions_ + 1;
        Partition part(psize);
        for (std::size_t p = 0; p < partitions_; ++p)
            cache_.push_back(Partition(psize));
    }

    // Inserts if not existing, replaces the passed value if existing.
    void
    set(Key const& key, std::shared_ptr<Value>& value)
    {
        std::cerr << "lru set: " << key << '\n';
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        {
            Partition &p = cache_[partitioner(key, partitions_)];
            std::lock_guard l(p.mtx);

            auto found = p.map.find(key);
            if (found == p.map.end())
            {
                p.map[key] = {p.enqueue(key, value), 1};
            }
            else
            {
                ++found->second.second;
                value = found->second.first->second;
                found->second.first = p.enqueue(key, value);
            }

//            p.map[key] = value;
//            auto found = p.map.find(key);
//            if (found == p.map.end())
//                p.evict();
//            else
//                p.q.erase(found->second);
//            p.q.push_front({key, value});
//            p.map[key] = p.q.begin();
        }
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
    }

    // Inserts or replaces the caller value with the existing
    // entry
//    void
//    setReplaceCaller(Key const& key, Value& value)
//    {
//        ++accesses_;
//        auto const startTime = std::chrono::steady_clock::now();
//        Partition &p = cache_[partitioner(key, partitions_)];
//        {
//            Partition &p = cache_[partitioner(key, partitions_)];
//            std::lock_guard l(p.mtx);
//            p.evict();
//
//            auto found = p.map.find(key);
//            p.q.push_back({key, value});
//            if (found == p.map.end())
//                p.map[key] = {p.q.back(), 1};
//            else
//                p.map[key] = {p.q.back(), found->second.second + 1};
//
//
//            std::lock_guard l(p.mtx);
//            auto found = p.map.find(key);
//            if (found == p.map.end())
//            {
//                p.map[key] = value;
//                p.evict();
//            }
//            else
//            {
//                value = found->second;
//                value = found->second->second;
//                p.q.erase(found->second);
//            }
//            p.q.push_front({key, value});
//            p.map[key] = p.q.begin();
//        }
//        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
//            std::chrono::steady_clock::now() - startTime).count();
//    }

    std::shared_ptr<Value>
    get(Key const& key)
    {
        std::cerr << "lru get " << key << ' ';
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        std::lock_guard l(p.mtx);
        auto found = p.map.find(key);
        if (found == p.map.end())
        {
            ++misses_;
            durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - startTime).count();
            std::cerr << "empty\n'";
            return {};
        }
        for (auto& e : p.map)
            std::cerr << "lru key: " << e.first << '\n';
        ++found->second.second;
        auto v = found->second.first->second;
        p.enqueue(key, v);
//        p.enqueue(key, found->second.first->second);
        ++hits_;
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
        std::cerr << "found\n";
        return found->second.first->second;
    }

    void
    del(Key const& key)
    {
        std::cerr << "lru del " << key << '\n';
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        std::lock_guard l(p.mtx);
        p.map.erase(key);
//        auto const& found = p.map.find(key);
//        if (found == p.map.end())
//            return;
//        p.q.erase(found->second);
//        p.map.erase(found);
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
    }

    std::size_t getEvicted()
    {
        std::size_t t = 0;
        for (auto& p : cache_)
        {
            std::lock_guard l(p.mtx);
            t += p.evicted;
        }
        return t;
    }

    std::size_t
    hits() const
    {
        return hits_;
    }

    std::size_t
    misses() const
    {
        return misses_;
    }

    std::size_t
    accesses() const
    {
        return accesses_;
    }

    std::size_t
    durationNs() const
    {
        return durationNs_;
    }

private:
    std::size_t partitions_;
    mutable std::vector<Partition> cache_;

    std::atomic<std::size_t> hits_{0};
    std::atomic<std::size_t> misses_{0};
    std::atomic<std::size_t> accesses_{0};
    std::atomic<std::size_t> durationNs_{0};
};

} // ripple

#endif // RIPPLE_BASICS_LRU_H
