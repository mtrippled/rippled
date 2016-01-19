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

#ifndef RIPPLE_BASICS_PARTITIONED_MAP_H_INCLUDED
#define	RIPPLE_BASICS_PARTITIONED_MAP_H_INCLUDED

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
    std::size_t(*Partitioner)(Key const&),
//    class Partitioner,
    class Compare = std::less<Key>,
    class Alloc = std::allocator<std::pair<const Key,T>>
    >
class partitioned_map
{
public:
    template <bool IsConst=false>
    class partitioned_map_iterator
        : public std::iterator <
            std::input_iterator_tag,
            typename partitioned_map::value_type,
            typename partitioned_map::difference_type,
            typename std::conditional<IsConst,
                typename partitioned_map::const_pointer,
                typename partitioned_map::pointer>::type,
            typename std::conditional<IsConst,
                typename partitioned_map::const_reference,
                typename partitioned_map::reference>::type
            >
    {
        friend class partitioned_map_iterator<true>;
        friend class partitioned_map_iterator<false>;

    public:
        using value_type =
            typename std::iterator_traits<partitioned_map_iterator>::value_type;
        using pointer =
            typename std::iterator_traits<partitioned_map_iterator>::pointer;
        using reference =
            typename std::iterator_traits<partitioned_map_iterator>::reference;
        using map_type =
            typename std::conditional<IsConst, typename partitioned_map::map_type const, typename partitioned_map::map_type>::type;
        using map_iterator =
                typename std::conditional<IsConst, typename std::map<Key, T, Compare, Alloc>::const_iterator, typename std::map<Key, T, Compare, Alloc>::iterator>::type;

    private:
        map_type* map_ = nullptr;
//        std::array<std::map<Key, T, Compare, Alloc>, Partitions> * map_ = nullptr;
//        pointer map_ = nullptr;
        std::size_t partition_ = 0;
//        typename std::map<Key, T, Compare, Alloc>::iterator it_;
        map_iterator it_;

        partitioned_map_iterator&
        inc()
        {
            ++it_;

            while (it_ == map_->at(partition_).end())
            {
                if (partition_ == Partitions - 1)
                    return *this;

                it_ = map_->at(++partition_).begin();
            }

            return *this;
        }

    public:
        partitioned_map_iterator()
        {}

        partitioned_map_iterator (map_type& map)
            : map_ (&map)
        {}

        partitioned_map_iterator (partitioned_map_iterator<false> const& other)
            : map_ (other.map_)
            , partition_ (other.partition_)
            , it_ (other.it_)
        {}

        partitioned_map_iterator&
        operator= (partitioned_map_iterator const& other) = default;

        bool
        operator== (partitioned_map_iterator const& other) const
        {
            return (partition_ == other.partition_) && (map_ == other.map_) &&
                (it_ == other.it_);
        }

        bool
        operator!= (partitioned_map_iterator const& other) const
        {
            return ! (*this == other);
        }

        reference
        operator*() const
        {
            return *it_;
        }

        pointer
        operator->() const
        {
            return &*it_;
        }

        partitioned_map_iterator&
        operator++()
        {
            return inc();
        }

        partitioned_map_iterator&
        operator++ (int)
        {
            iterator tmp(*this);
            inc();
            return tmp;
        }

        partitioned_map_iterator&
        begin()
        {
            partition_ = 0;
            it_ = map_->at(partition_).begin();
            return *this;
        }

        partitioned_map_iterator&
        end()
        {
            partition_ = Partitions - 1;
            it_ = map_->at(partition_).end();
            return *this;
        }

        partitioned_map_iterator&
        find (typename partitioned_map::key_type const& key)
        {
            partition_ = Partitioner(key);
            it_ = map_->at(partition_).find(key);

            if (it_ == map_->at(partition_).end())
                return end();

            return *this;
        }

        partitioned_map_iterator&
        upper_bound (typename partitioned_map::key_type const& key)
        {
            partition_ = Partitioner(key);
            it_ = map_->at(partition_).upper_bound(key);

            while (it_ == map_->at(partition_).end())
            {
                if (partition_ == Partitions - 1)
                    return end();

                it_ = map_->at(++partition_).begin();
            }

            return *this;
        }

        partitioned_map_iterator&
        erase (typename partitioned_map::const_iterator it)
        {
//            return it.map_->at(it.partition_).erase(it.it_);
            map_->at(partition_).erase(it.it_);
//            ++it.partition_;
//            it.map_ = nullptr;
//            it.it_ = *this;
            return *this;
        }

//        template <class... Keyed, class... Mapped>
        template <class... Urgs>
        std::pair<partitioned_map_iterator, bool>
        emplace (Urgs&&... urgs)
//        emplace (std::pair<Keyed..., Mapped...> foo)
//        emplace (std::piecewise_construct_t const& p,
//            Keyed keyed,
//            std::tuple<typename partitioned_map::key_type const> const& key,
//            Mapped mapped)
        {
            std::tuple<Urgs...> a(urgs...);

            if (sizeof...(urgs) == 3)
            {
                partition_ = Partitioner(std::get<0>(std::get<1>(a)));
                auto const b = std::get<0>(std::get<1>(a));
                auto const c = std::get<0>(std::get<2>(a));
                auto const d = std::get<1>(std::get<2>(a));
                typename std::tuple_element<2, std::tuple<Urgs...>>::type lv = std::get<2>(a);
                auto s = map_->at(partition_).emplace(
                    std::piecewise_construct, std::get<1>(a), lv);
//                    std::piecewise_construct, std::get<1>(a), std::get<2>(a));
//                    std::piecewise_construct, std::get<1>(a), std::make_tuple(c, d));
                it_ = s.first;
                return std::make_pair(*this, s.second);
            }
            else
            {
                // TODO as other forms of emplace() are used.
            }
        }

        /*
        std::pair<partitioned_map_iterator, bool>
        emplace (typename partitioned_map::key_type const &key,
            typename partitioned_map::mapped_type const& mapped)
        {
            return emplace(std::piecewise_construct, std::forward_as_tuple(key),
                std::forward_as_tuple(mapped));
        }

        std::pair<partitioned_map_iterator, bool>
        emplace (std::pair<typename partitioned_map::key_type const,
            typename partitioned_map::mapped_type> const& value)
        {
            return emplace(std::piecewise_construct,
                std::forward_as_tuple(value.first),
                std::forward_as_tuple(value.second));
        }
         */
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
    using iterator        = partitioned_map_iterator<false>;
    using const_iterator  = partitioned_map_iterator<true>;
    using map_type        = std::array<std::map<Key, T, Compare, Alloc>,
                                Partitions>;

private:
    map_type map_;

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
        return const_iterator(map_).upper_bound(key);
    }

    iterator&
    erase (iterator it)
    {
        return it.erase(it);
//        return iterator(it).erase(it);
    }

    const_iterator&
    erase (const_iterator it) const
    {
        return it.erase(it);
//        return const_iterator(it).erase(it);
    }

    template <class... Args>
    std::pair<iterator, bool>
    emplace (Args&&... args)
    {
        return iterator(map_).emplace(args...);
    }
};

//------------------------------------------------------------------------------

std::size_t
partitioner (uint256 const& key);

} // ripple

#endif // RIPPLE_BASICS_PARTITIONED_MAP_H_INCLUDED
