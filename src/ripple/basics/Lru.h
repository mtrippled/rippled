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
    struct Entry
    {
        std::shared_ptr<Value> val;
        std::size_t ref {1};

        Entry(std::shared_ptr<Value> const& v)
            : val(v)
        {}

        Entry(Entry const& other)
        {
            val = other.val;
            ref = other.ref;
        }

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

    using map_type = std::unordered_map<Key, Value>;
//    using map_type = std::unordered_map<Key, Entry>;
//    using q_type = boost::circular_buffer<Entry>;
    using q_type = boost::circular_buffer<typename map_type::iterator>;
//    using Entry_old = std::pair<Key, std::shared_ptr<Value>>;
//    using q_type = boost::circular_buffer<Entry_old>;
//    using map_type = std::unordered_map<Key,
//          std::pair<typename q_type::iterator, std::size_t>,
//                                        Hash, Pred>;

private:
    struct Partition
    {
        std::size_t capacity;
        q_type q;
        map_type map;
        std::mutex mtx;
        std::size_t evicted {0};

        Partition(std::size_t const cap)
//            : capacity(cap)
//            , q(q_type(cap))
        {
//            map.reserve(cap);
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
//        cache_.reserve(partitions_);
//        std::size_t const psize = capacity / partitions_ + 1;
//        for (std::size_t p = 0; p < partitions_; ++p)
//            cache_.push_back(std::move(Partition(psize)));
    }

    // Inserts if not existing, replaces the passed value if existing.
    void
    set(Key const& key, std::shared_ptr<Value>& value)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        {
            std::size_t const partNum = partitioner(key, partitions_);
            Partition &p = cache_[partNum];
            std::lock_guard l(p.mtx);

//            auto found = p.map.find(key);
//            if (found == p.map.end())
            {
//                p.map[key] = Entry(value);
                // CHECK BUFFER
//                p.q.push_front(p.map.find(value));
//                auto v = p.enqueue(key, value);
//                p.map[key] = {v, 1};
            }
//            else
            {
//                ++found->second.ref;
//                ++found->second.second;
                // CHECK BUFFER
//                value = found->second.value();
//                value = found->second.first->second;
//                found->second.first = p.enqueue(key, value);
            }
        }
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
    }

    std::shared_ptr<Value>
    get(Key const& key)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        std::size_t const partNum = partitioner(key, partitions_);
        Partition& p = cache_[partNum];
        std::lock_guard l(p.mtx);
//        auto found = p.map.find(key);
//        if (found == p.map.end())
        {
            ++misses_;
            durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - startTime).count();
            return {};
        }
//        ++found->second.ref;
//        ++found->second.second;
        // CHECK BUFFER
//        p.q.push_back(found);
//        auto v = p.enqueue(key, found->second.first->second);
//        p.map[key] = {v, found->second.second};
        ++hits_;
        durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - startTime).count();
//        return found->second.value();
//        return found->second.first->second;
        return {};
    }

    void
    del(Key const& key)
    {
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        std::size_t const partNum = partitioner(key, partitions_);
        Partition& p = cache_[partNum];
        std::lock_guard l(p.mtx);
//        auto found = p.map.find(key);
//        if (found != p.map.end())
//            found->second.del();
//        p.map.erase(key);
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
