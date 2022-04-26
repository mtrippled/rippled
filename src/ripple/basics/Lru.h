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
            std::cerr << "lru q cap " << cap << '\n';
            std::cerr << "lru q capacity " << q.capacity() << '\n';
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
            std::cerr << "lru enqueue " << key << '\n';
            std::cerr << "lru enqueue q size " << q.size() << '\n';
            std::cerr << "lru enqueue capacity " << q.capacity() << '\n';
            if (q.size() == capacity)
            {
                auto found = map.find(q.back().first);
                if (found != map.end() && --found->second.second == 0)
                    map.erase(found);
                ++evicted;
            }
//            q.push_front({key, value});
            std::shared_ptr<Value> v;
            q.push_front({key, v});
            std::cerr << "lru enqueued q size " << q.size() << '\n';
            std::cerr << "lru enqueued capacity " << q.capacity() << '\n';
            std::cerr << "lru enqueued key " << q.begin()->first << '\n';
            std::cerr << "lru enqueued value " << q.begin()->second.get() << '\n';
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
        boost::circular_buffer<int> cb(capacity);
        cb.push_front(5);
        std::cerr << "lru cb size: " << cb.size() << '\n';
        boost::circular_buffer<uint256> cb2(capacity);
        uint256 foo;
        cb2.push_front(foo);
        std::cerr << "lru cb2 size: " << cb2.size() << '\n';
        boost::circular_buffer<
            std::pair<uint256, std::shared_ptr<SHAMapTreeNode>>> cb3(capacity);

        uint256 big;
        Slice slice(big.data(), 32);
        std::shared_ptr<SHAMapItem> sp = std::make_shared<SHAMapItem>(big, slice);
        SHAMapTxLeafNode leaf(sp, 0);
        std::shared_ptr<SHAMapTxLeafNode> leafsp;
//        auto leafsp = std::make_shared<SHAMapTxLeafNode>(std::move(leaf));
        cb3.push_front({foo, leafsp});
        std::cerr << "lru cb3 size: " << cb3.size() << '\n';
        boost::circular_buffer<std::pair<uint256, std::shared_ptr<NodeObject>>> cb4(capacity);
        std::shared_ptr<NodeObject> no;
        cb4.push_front({foo, no});
        std::cerr << "lru cb4 size: " << cb4.size() << '\n';
        boost::circular_buffer<std::pair<uint256, std::shared_ptr<char>>> cb5(capacity);
        cb5.push_front({foo, std::make_shared<char>('a')});
        std::cerr << "lru cb5 size: " <<cb5.size() << '\n';

//        std::vector<SHAMapTreeNode>;

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
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        {
            Partition &p = cache_[partitioner(key, partitions_)];
            std::lock_guard l(p.mtx);
            std::cerr << "lru wtf set: " << key << '\n';

            auto found = p.map.find(key);
            if (found == p.map.end())
            {
                std::cerr << "lru wtf eq1 " << key << '\n';
                p.map[key] = {p.enqueue(key, value), 1};
            }
            else
            {
                ++found->second.second;
                value = found->second.first->second;
                std::cerr << "lru eq2 " << key << '\n';
                found->second.first = p.enqueue(key, value);
            }

            auto foo = p.map.find(key);
            std::cerr << "lru set refcount: " << foo->first << ',' << foo->second.second << '\n';
            auto v = foo->second.first->second;
            std::cerr << "lru v: " << v.get() << '\n';

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
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        std::lock_guard l(p.mtx);
        std::cerr << "lru get " << key << ' ';
        auto found = p.map.find(key);
        if (found == p.map.end())
        {
            ++misses_;
            durationNs_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - startTime).count();
            std::cerr << "empty\n";
            return {};
        }
        for (auto& e : p.map)
            std::cerr << "lru key,refcount: " << e.first << ',' << found->second.second << '\n';
        ++found->second.second;
        auto v = found->second.first->second;
        std::cerr << "lru eq3 " << key << '\n';
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
        ++accesses_;
        auto const startTime = std::chrono::steady_clock::now();
        Partition& p = cache_[partitioner(key, partitions_)];
        std::lock_guard l(p.mtx);
        std::cerr << "lru del " << key << '\n';
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
