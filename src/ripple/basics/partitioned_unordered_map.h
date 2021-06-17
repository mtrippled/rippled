//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2018 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H
#define RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H

#include <ripple/basics/Log.h>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

/*
template <
    class Key,
    class Value,
    class Hash = hardened_hash<strong_hash>,
    class Pred = std::equal_to<Key>,
    class Allocator = std::allocator<std::pair<Key const, Value>>>
using hardened_hash_map = std::unordered_map<Key, Value, Hash, Pred, Allocator>;
*/

namespace ripple {

template<typename Key,
    typename Value,
    typename Hash = std::hash<Key>,
    typename Pred = std::equal_to<Key>,
    typename Alloc = std::allocator<std::pair<const Key, Value>>>
class partitioned_unordered_map
{
    std::function<std::uint64_t(Key const&)> extractor_;
//    std::function<std::uint64_t(Key const&)> extractor_ {
//        [](Key const& key) {
//            return static_cast<std::uint64_t>(key);
//        }
//    };

    std::size_t partitions_;
//    std::size_t partitionBits_ {0};

public:
    using key_type = Key;
    using mapped_type = Value;
    using value_type = std::pair<Key const, mapped_type>;
    using size_type = std::size_t;
    using difference_type = std::size_t;
    using hasher = Hash;
    using key_equal = Pred;
    using allocator_type = Alloc;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    // iterator =
    // const_iterator =
    // local_iterator =
    // const_local_iterator =
    // node_type =
    // insert_return_type =

    using map_type = std::unordered_map<key_type, mapped_type, hasher,
                                        key_equal, allocator_type>;
    using partition_map_type = std::vector<map_type>;

    struct iterator
    {
        using iterator_category = std::forward_iterator_tag;

        partition_map_type* map_ {nullptr};
        typename partition_map_type::iterator ait_;
        typename map_type::iterator mit_;

        iterator() = default;

        iterator(partition_map_type* map)
            : map_(map)
        {}

        reference
        operator*() const
        {
            return *mit_;
        }

        pointer
        operator->() const
        {
            return &(*mit_);
        }

        void
        inc()
        {
            ++mit_;
            while (mit_ == ait_->end())
            {
                ++ait_;
                if (ait_ == map_->end())
                    return;
                mit_ = ait_->begin();
            }
        }

        // ++it
        iterator&
        operator++()
        {
            inc();
            return *this;
        }

        // it++
        iterator
        operator++(int)
        {
            iterator tmp(*this);
            inc();
            return tmp;
        }

        friend bool
        operator==(iterator const& lhs, iterator const& rhs)
        {
            return lhs.map_ == rhs.map_ && lhs.ait_ == rhs.ait_ &&
                   lhs.mit_ == rhs.mit_;
        }

        friend bool
        operator!=(iterator const& lhs, iterator const& rhs)
        {
            return !(lhs == rhs);
        }
    };

    struct const_iterator
    {
        using iterator_category = std::forward_iterator_tag;

        partition_map_type* map_ {nullptr};
        typename partition_map_type::iterator ait_;
        typename map_type::iterator mit_;

        const_iterator() = default;

        const_iterator(partition_map_type* map)
        : map_(map)
        {}

        const_iterator(iterator& orig)
        {
            map_ = orig.map_;
            ait_ = orig.ait_;
            mit_ = orig.mit_;
        }

        const_reference
        operator*() const
        {
            return *mit_;
        }

        const_pointer
        operator->() const
        {
            return &(*mit_);
        }

        void
        inc()
        {
            ++mit_;
            while (mit_ == ait_->end())
            {
                ++ait_;
                if (ait_ == map_->end())
                    return;
                mit_ = ait_->begin();
            }
        }

        // ++it
        const_iterator&
        operator++()
        {
            inc();
            return *this;
        }

        // it++
        const_iterator
        operator++(int)
        {
            const_iterator tmp(*this);
            inc();
            return tmp;
        }

        friend bool
        operator==(const_iterator const& lhs, const_iterator const& rhs)
        {
            return lhs.map_ == rhs.map_ && lhs.ait_ == rhs.ait_ &&
                   lhs.mit_ == rhs.mit_;
        }

        friend bool
        operator!=(const_iterator const& lhs, const_iterator const& rhs)
        {
            return !(lhs == rhs);
        }
    };

private:
    template <class T>
    static void
    end(T& it)
    {
        it.ait_ = it.map_->end();
        it.mit_ = it.map_->back().end();
    }

    template <class T>
    static void
    begin(T& it)
    {
        for (it.ait_ = it.map_->begin(); it.ait_ != it.map_->end(); ++it.ait_)
        {
            if (it.ait_->begin() == it.ait_->end())
                continue;
            it.mit_ = it.ait_->begin();
            return;
        }
        end(it);
    }

public:
    partitioned_unordered_map(std::function<std::uint64_t(Key const&)> f,
            std::optional<std::size_t> partitions = std::nullopt)
        : extractor_(f)
    {
        partitions_ = partitions && *partitions ? *partitions :
                        std::thread::hardware_concurrency() * 2;
        map_.resize(partitions_);
        assert(partitions_);

        //        std::size_t p = partitions && *partitions ? *partitions :
//            std::thread::hardware_concurrency() * 2;
//
//        std::size_t v = 1;
//        while (v < p)
//        {
//            v <<= 1;
//            ++partitionBits_;
//        }
//        partitions_ = v;
//        std::cerr << "p,v,bits,partitions_: " << p << ',' << v << ','
//                  << partitionBits_ << ',' << partitions_ << '\n';
    }

    std::size_t
    partitions() const
    {
        return partitions_;
    }

    partition_map_type&
    map()
    {
        return map_;
    }

    iterator
    begin()
    {
        iterator it(&map_);
        begin(it);
        return it;
    }

    const_iterator
    cbegin() const
    {
        const_iterator it(&map_);
        begin(it);
        return it;
    }

    const_iterator
    begin() const
    {
        return cbegin();
    }

    iterator
    end()
    {
        iterator it(&map_);
        end(it);
        return it;
    }

    const_iterator
    cend() const
    {
        const_iterator it(&map_);
        end(it);
        return it;
    }

    const_iterator
    end() const
    {
        return cend();
    }

private:
    std::uint64_t
    partitioner(key_type const& key) const
    {
//        static std::uint8_t const shift = 64 - partitionBits_;
        return extractor_(key) % partitions_;
//        return extractor_(key) >> shift;
//        return *reinterpret_cast<std::uint64_t const*>(key.data()) >> shift;
    }

    template <class T>
    void
    find(key_type const& key, T& it) const
    {
        it.ait_ = it.map_->begin() + partitioner(key);
        it.mit_ = it.ait_->find(key);
        if (it.mit_ == it.ait_->end())
            end(it);
    }

public:
    iterator
    find(key_type const& key)
    {
        iterator it(&map_);
        find(key, it);
        return it;
    }

    const_iterator
    find(key_type const& key) const
    {
        const_iterator it(&map_);
        find(key, it);
        return it;
    }

    template <class T, class U>
    std::pair<iterator, bool>
    emplace(std::piecewise_construct_t const&, T&& keyTuple, U&& valueTuple)
    {
        auto& key = std::get<0>(keyTuple);
        iterator it(&map_);
        it.ait_ = it.map_->begin() + partitioner(key);
        auto emplaced = it.ait_->emplace(
            std::piecewise_construct,
            std::move(keyTuple),
            std::move(valueTuple));
        it.mit_ = emplaced.first;
        return {it, emplaced.second};
    }

    void
    clear()
    {
        for (auto& p : map_)
            p.clear();
    }

    iterator
    erase(const_iterator position)
    {
        iterator it(&map_);
        it.ait_ = position.ait_;
        it.mit_ = position.ait_->erase(position.mit_);

        while (it.mit_ == it.ait_->end())
        {
            ++it.ait_;
            if (it.ait_ == it.map_->end())
                break;
            it.mit_ = it.ait_->begin();
        }

        return it;
    }

    std::size_t
    size() const
    {
        std::size_t ret = 0;
        for (auto& p : map_)
            ret += p.size();
        return ret;
    }

private:
    mutable partition_map_type map_{};
};

} // namespace ripple

#endif  // RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H
