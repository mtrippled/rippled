//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2016 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_PARALLEL_MAP_H_INCLUDED
#define	RIPPLE_BASICS_PARALLEL_MAP_H_INCLUDED

#include <ripple/basics/base_uint.h>
#include <cstdarg>
#include <iterator>
#include <map>
#include <tuple>
#include <utility>

namespace ripple {

template <
    class Key,
    class T,
    std::size_t Partitions,
    class Partitioner,
    class Compare = std::less<Key>,
    class Alloc = std::allocator<std::pair<const Key,T>>
    >
class parallel_map
{
public:
    template <bool IsConst=false>
    class parallel_map_iterator
        : public std::iterator <
            std::input_iterator_tag,
            typename parallel_map::value_type,
            typename parallel_map::difference_type,
            typename std::conditional<IsConst,
                typename parallel_map::const_pointer,
                typename parallel_map::pointer>::type,
            typename std::conditional<IsConst,
                typename parallel_map::const_reference,
                typename parallel_map::reference>::type
            >
    {
    public:
        using value_type =
            typename std::iterator_traits<parallel_map_iterator>::value_type;
        using pointer =
            typename std::iterator_traits<parallel_map_iterator>::pointer;
        using reference =
            typename std::iterator_traits<parallel_map_iterator>::reference;

    private:
        pointer map_ = nullptr;
        std::size_t partition_ = 0;
        typename std::map<Key, T, Compare, Alloc>::iterator it_;

        parallel_map_iterator&
        inc()
        {
            ++it_;

            while (it_ == map_->map_[partition_].end())
            {
                if (partition_ == Partitions - 1)
                    return *this;

                it_ = map_->map_[++partition_].begin();
            }

            return *this;
        }

    public:
        parallel_map_iterator (parallel_map<Key, T, Partitions, Partitioner,
                Compare, Alloc>& map)
            : map_ (&map)
        {}

        parallel_map_iterator (parallel_map_iterator<false> const& other)
            : map_ (other.map_)
            , partition_ (other.partition_)
            , it_ (other.it_)
        {}

        parallel_map_iterator (parallel_map_iterator<IsConst> const& other)
            : map_ (other.map_)
            , partition_ (other.partition_)
            , it_ (other.it_)
        {}

        parallel_map_iterator&
        operator= (parallel_map_iterator const& other) = default;

        bool
        operator== (parallel_map_iterator const& other) const
        {
            return (partition_ == other.partition_) && (map_ == other.map_) &&
                (it_ == other.it_);
        }

        bool
        operator!= (parallel_map_iterator const& other) const
        {
            return ! (*this == other);
        }

        reference
        operator*()
        {
            return *it_;
        }

        pointer
        operator->()
        {
            return &*it_;
        }

        parallel_map_iterator&
        operator++()
        {
            return inc();
        }

        parallel_map_iterator&
        operator++ (int)
        {
            iterator tmp(*this);
            inc();
            return tmp;
        }

        parallel_map_iterator&
        begin()
        {
            partition_ = 0;
            it_ = map_->map_[partition_].begin();
            return *this;
        }

        parallel_map_iterator&
        end()
        {
            partition_ = Partitions - 1;
            it_ = map_->map_[partition_].end();
            return *this;
        }

        parallel_map_iterator&
        find (typename parallel_map::key_type const& key)
        {
            partition_ = Partitioner(key);
            it_ = map_->map_[partition_].find(key);

            if (it_ == map_->map_[partition_].end())
                return end();

            return *this;
        }

        parallel_map_iterator&
        upper_bound (typename parallel_map::key_type const& key)
        {
            partition_ = Partitioner(key);
            it_ = map_->map_[partition_].upper_bound(key);

            while (it_ == map_->map_[partition_].end())
            {
                if (partition_ == Partitions - 1)
                    return end();

                it_ = map_->map_[++partition_].begin();
            }

            return *this;
        }

        parallel_map_iterator&
        erase (typename parallel_map::const_iterator it)
        {
            return it.map_->at(it.partition_).erase(it.it_);
        }

        std::pair<parallel_map_iterator, bool>
        emplace (std::tuple<typename parallel_map::key_type const> const& key,
            std::tuple<typename parallel_map::mapped_type> const& mapped)
        {
            partition_ = Partition(std::get<0>(key));
            auto s = map_->map_[partition_].emplace(std::piecewise_construct,
                key, mapped);
            it_ = s.first;
            return std::make_pair(*this, s.second);
        }

        std::pair<parallel_map_iterator, bool>
        emplace (typename parallel_map::key_type const &key,
            typename parallel_map::mapped_type const& mapped)
        {
            return emplace(std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(mapped));
        }

        std::pair<parallel_map_iterator, bool>
        emplace (std::pair<typename parallel_map::key_type const,
            typename parallel_map::mapped_type> const& value)
        {
            return emplace(std::piecewise_construct,
                std::forward_as_tuple(value.first),
                std::forward_as_tuple(value.second));
        }
};

public:
    using key_type        = Key;
    using mapped_type     = T;
    using value_type      = std::pair<key_type const, mapped_type>;
    using reference       = value_type&;
    using const_reference = value_type const&;
    using pointer         = value_type*;
    using const_pointer   = value_type const*;
    using difference_type = std::ptrdiff_t;
    using iterator        = parallel_map_iterator<false>;
    using const_iterator  = parallel_map_iterator<true>;

private:
    std::array<std::map<Key, T, Compare, Alloc>, Partitions> map_;

public:
    iterator&
    begin()
    {
        return iterator(map_).begin();
    }

    const_iterator&
    begin() const
    {
        return const_iterator(map_).begin();
    }

    iterator&
    end()
    {
        return iterator(map_).end();
    }

    const_iterator&
    end() const
    {
        return const_iterator(map_).end();
    }

    iterator&
    find (key_type const& key)
    {
        return iterator(map_).find(key);
    }

    const_iterator&
    find (key_type const& key) const
    {
        return const_iterator(map_).find(key);
    }

    iterator&
    upper_bound (key_type const& key)
    {
        return iterator(map_).upper_bound(key);
    }

    const_iterator&
    upper_bound (key_type const& key) const
    {
        return iterator(map_).upper_bound(key);
    }

    iterator&
    erase (const_iterator it)
    {
        return iterator(it).erase(it);
    }

    const_iterator&
    erase (const_iterator it) const
    {
        return const_iterator(it).erase(it);
    }

    template <class... Args>
    std::pair<iterator, bool>
    emplace (Args&&... args)
    {
        return iterator(map_).emplace(args...);
    }

    template <class... Args>
    std::pair<const_iterator, bool>
    emplace (Args&&...args)
    {
        return const_iterator(map_).emplace(args...);
    }
};

//------------------------------------------------------------------------------

inline std::size_t
partitioner (uint256 const& key);

} // ripple

#endif // RIPPLE_BASICS_PARALLEL_MAP_H_INCLUDED
