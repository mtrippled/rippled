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
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <iostream>
#include <sstream>
#include <ripple/basics/base_uint.h>
#include <ripple/shamap/SHAMapTreeNode.h>
#include <ripple/shamap/SHAMapLeafNode.h>
#include <ripple/shamap/SHAMapItem.h>
#include <ripple/basics/Slice.h>
#include <ripple/shamap/SHAMapTxLeafNode.h>
#include <ripple/nodestore/NodeObject.h>

namespace ripple {

template <
    typename Key,
    typename Value,
    typename Hash = hardened_hash<beast::xxhasher>,
    typename Pred = std::equal_to<Key>>
class Lru
{
public:
    /*
    struct Entry
    {
        std::shared_ptr<Value> val;
        std::size_t ref {1};

        Entry(std::shared_ptr<Value> const& v)
            : val(v)
        {}

        void
        del()
        {
            val.reset();
        }

        std::shared_ptr<Value>
        value()
        {
            return val;
        }
    };
     */

    using Entry = std::pair<std::shared_ptr<Value>, std::size_t>;
    using map_type = std::unordered_map<Key, Entry, Hash, Pred>;
//    using map_type = std::unordered_map<Key, std::shared_ptr<Value>, Hash, Pred>;
    using q_type = boost::circular_buffer<typename map_type::iterator>;

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

        Partition(Partition const& other)
        {
            capacity = other.capacity;
            q = other.q;
            map = other.map;
            evicted = other.evicted;
        }

        void
        enqueue(typename map_type::iterator newIt)
        {
            return;
            static std::size_t const qCap = capacity;
            if (q.size() == qCap)
            {
                auto& oldIt = q.back();
                auto& entry = oldIt->second;
                if (--entry.second == 0)
                    map.erase(oldIt);
                ++evicted;
            }
            q.push_front(newIt);
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
        for (std::size_t p = 0; p < partitions_; ++p)
            cache_.push_back(std::move(Partition(psize)));
    }

    // Inserts if not existing, replaces the passed value if existing.
    void
    set(Key const& key, std::shared_ptr<Value>& value)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        {
            Partition &p = cache_[partitioner(key, partitions_)];
            std::lock_guard l(p.mtx);

            auto [it, inserted] = p.map.emplace(std::piecewise_construct,
                                                std::forward_as_tuple(key),
                                                std::forward_as_tuple(std::make_pair(value, 1)));
            if (!inserted)
            {
                ++it->second.second;
                value = it->second.first;
            }
            p.enqueue(it);

            /* orig
            auto [it, inserted] = p.map.emplace(std::piecewise_construct,
                                         std::forward_as_tuple(key),
                                         std::forward_as_tuple(Entry(value)));
            if (!inserted)
            {
                ++it->second.ref;
                value = it->second.value();
            }
            p.enqueue(it);
             */
        }
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
    }

    std::shared_ptr<Value>
    get(Key const& key)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        std::lock_guard l(p.mtx);

        auto found = p.map.find(key);
        if (found == p.map.end() || !found->second.first )
        {
            ++misses_;
            durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() - startTime).count();
            return {};
        }
        ++found->second.second;
        p.enqueue(found);

        /* orig
        auto found = p.map.find(key);
        if (found == p.map.end())
        {
            ++misses_;
            durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - startTime).count();
            return {};
        }
        ++found->second.ref;
        p.enqueue(found);
         */
        ++hits_;
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
        return found->second.first;
//        return found->second.value(); orig
    }

    void
    del(Key const& key)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        {
            std::lock_guard l(p.mtx);
            auto found = p.map.find(key);
            if (found != p.map.end())
                found->second.first.reset();

            /* orig
            auto found = p.map.find(key);
            if (found != p.map.end())
                found->second.del();
                */
        }
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

    std::size_t
    size()
    {
        std::size_t ret = 0;
        for (Partition& partition : cache_)
        {
            std::lock_guard lock(partition.mtx);
            ret += partition.map.size();
        }
        return ret;
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
