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

#ifndef RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H
#define RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H

#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ripple {

template <typename Key>
std::size_t
partitioner(Key const& key, std::size_t const numPartitions);

template <
    typename Key,
    typename Value,
    typename Hash,
    typename Pred = std::equal_to<Key>,
    typename Alloc = std::allocator<std::pair<const Key, Value>>>
class partitioned_unordered_map
{
    std::size_t const numPartitions_;

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
    using map_type = std::
        unordered_map<key_type, mapped_type, hasher, key_equal, allocator_type>;
    using mutex_type = std::recursive_mutex;

    struct Partition
    {
        map_type map;
        mutable mutex_type mutex;

        Partition() = default;

        Partition(Partition const& other)
        {
            std::lock_guard lock(other.mutex);
            map = other.map;
        }
    };

    using partition_map_type = std::vector<Partition>;

    struct iterator
    {
        using iterator_category = std::forward_iterator_tag;
        partition_map_type* partitions_{nullptr};
        typename partition_map_type::iterator vectorIt_;
        typename map_type::iterator partitionIt_;

        iterator() = default;

        iterator(partition_map_type* map) : partitions_(map)
        {
        }

        reference
        operator*() const
        {
            return *partitionIt_;
        }

        pointer
        operator->() const
        {
            return &(*partitionIt_);
        }

        void
        inc()
        {
            std::unique_lock lock(vectorIt_->mutex);
            ++partitionIt_;
            while (partitionIt_ == vectorIt_.map->end())
            {
                lock.unlock();
                ++vectorIt_;
                if (vectorIt_ == partitions_->end())
                    return;
                lock = std::unique_lock(vectorIt_->mutex);
                partitionIt_ = vectorIt_->map.begin();
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
            return lhs.partitions_ == rhs.partitions_ &&
                lhs.vectorIt_ == rhs.vectorIt_ &&
                lhs.partitionIt_ == rhs.partitionIt_;
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

        partition_map_type* partitions_{nullptr};
        typename partition_map_type::iterator vectorIt_;
        typename map_type::iterator partitionIt_;

        const_iterator() = default;

        const_iterator(partition_map_type* vectorOfMaps)
            : partitions_(vectorOfMaps)
        {
        }

        const_iterator(iterator& orig)
        {
            partitions_ = orig.partitions_;
            vectorIt_ = orig.vectorIt_;
            partitionIt_ = orig.partitionIt_;
        }

        const_reference
        operator*() const
        {
            return *partitionIt_;
        }

        const_pointer
        operator->() const
        {
            return &(*partitionIt_);
        }

        void
        inc()
        {
            std::unique_lock lock(vectorIt_->mutex);
            ++partitionIt_;
            while (partitionIt_ == vectorIt_->map.end())
            {
                lock.unlock();
                ++vectorIt_;
                if (vectorIt_ == partitions_->end())
                    return;
                lock = std::unique_lock(vectorIt_->mutex);
                partitionIt_ = vectorIt_->map.begin();
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
            return lhs.partitions_ == rhs.partitions_ &&
                lhs.vectorIt_ == rhs.vectorIt_ &&
                lhs.partitionIt_ == rhs.partitionIt_;
        }

        friend bool
        operator!=(const_iterator const& lhs, const_iterator const& rhs)
        {
            return !(lhs == rhs);
        }
    };

private:
    std::size_t
    partitioner(Key const& key) const
    {
        return ripple::partitioner(key, numPartitions_);
    }

    template <class T>
    static void
    end(T& it)
    {
        it.vectorIt_ = it.partitions_->end();
        std::lock_guard lock(it.partitions_->back().mutex);
        it.partitionIt_ = it.partitions_->back().map.end();
    }

    template <class T>
    static void
    begin(T& it)
    {
        for (it.vectorIt_ = it.partitions_->begin();
            it.vectorIt_ != it.partitions_->end();
            ++it.vectorIt_)
        {
            if (it.vectorIt_->map.begin() == it.vectorIt_->map.end())
                continue;
            std::lock_guard lock(it.vectorIt_->mutex);
            it.partitionIt_ = it.vectorIt_->map.begin();
            return;
        }
        end(it);
    }

public:
    // Set partitions to the number of hardware threads if the parameter
    // is either empty or set to 0.
    partitioned_unordered_map(
        std::optional<std::size_t> partitions = std::nullopt)
            : numPartitions_ (partitions && *partitions
                              ? *partitions
                              : std::thread::hardware_concurrency())
    {
        assert(numPartitions_);
        partitions_.resize(numPartitions_);
    }

    std::size_t
    numPartitions() const
    {
        return numPartitions_;
    }

    partition_map_type&
    partitions()
    {
        return partitions_;
    }

    iterator
    begin()
    {
        iterator it(&partitions_);
        begin(it);
        return it;
    }

    const_iterator
    cbegin() const
    {
        const_iterator it(&partitions_);
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
        iterator it(&partitions_);
        end(it);
        return it;
    }

    const_iterator
    cend() const
    {
        const_iterator it(&partitions_);
        end(it);
        return it;
    }

    const_iterator
    end() const
    {
        return cend();
    }

private:
    template <class T>
    void
    find(key_type const& key, T& it) const
    {
        it.vectorIt_ = it.partitions_->begin() + partitioner(key);
        std::unique_lock lock(it.vectorIt_->mutex);
        it.partitionIt_ = it.vectorIt_->map.find(key);
        if (it.partitionIt_ == it.vectorIt_->map.end())
        {
            lock.unlock();
            end(it);
        }
    }

public:
    iterator
    find(key_type const& key)
    {
        iterator it(&partitions_);
        find(key, it);
        return it;
    }

    const_iterator
    find(key_type const& key) const
    {
        const_iterator it(&partitions_);
        find(key, it);
        return it;
    }

    template <class T, class U>
    std::pair<iterator, bool>
    emplace(std::piecewise_construct_t const&, T&& keyTuple, U&& valueTuple)
    {
        auto const& key = std::get<0>(keyTuple);
        iterator it(&partitions_);
        it.vectorIt_ = it.partitions_->begin() + partitioner(key);

        std::pair<typename map_type::iterator, bool> emplaced;
        {
            std::lock_guard lock(it.vectorIt_->mutex);
            emplaced = it.vectorIt_->map.emplace(
                std::piecewise_construct,
                std::forward<T>(keyTuple),
                std::forward<U>(valueTuple));
        }
        it.partitionIt_ = emplaced.first;
        return {it, emplaced.second};
    }

    template <class T, class U>
    std::pair<iterator, bool>
    emplace(T key, U val)
    {
        iterator it(&partitions_);
        it.vectorIt_ = it.partitions_->begin() + partitioner(key);

        std::pair<typename map_type::iterator, bool> emplaced;
        {
            std::lock_guard lock(it.vectorIt_->mutex);
            emplaced = it.vectorIt_->map.emplace(
                std::forward<T>(key), std::forward<U>(val));
        }
        it.partitionIt_ = emplaced.first;
        return {it, emplaced.second};
    }

    void
    clear()
    {
        for (auto& p : partitions_)
        {
            std::lock_guard lock(p.mutex);
            p.map.clear();
        }
    }

    iterator
    erase(const_iterator position)
    {
        iterator it(&partitions_);
        it.vectorIt_ = position.vectorIt_;
        std::unique_lock lock(position.vectorIt_->mutex);
        it.partitionIt_ = position.vectorIt_->map.erase(
            position.partitionIt_);

        while (it.partitionIt_ == it.vectorIt_->map.end())
        {
            lock.unlock();
            ++it.vectorIt_;
            if (it.vectorIt_ == it.partitions_->end())
                break;
            lock = std::unique_lock(it.vectorIt_->mutex);
            it.partitionIt_ = it.vectorIt_->map.begin();
        }
        return it;
    }

    std::size_t
    size() const
    {
        std::size_t ret = 0;
        for (auto& p : partitions_)
        {
            std::lock_guard lock(p.mutex);
            ret += p.map.size();
        }
        return ret;
    }

    mutex_type&
    partitionMutex(key_type const& key)
    {
        return partitions_[partitioner(key)].mutex;
    }

private:
    mutable partition_map_type partitions_{};
};

}  // namespace ripple

#endif  // RIPPLE_BASICS_PARTITIONED_UNORDERED_MAP_H
