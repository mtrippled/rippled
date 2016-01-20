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

namespace detail {
template<int...> struct index_tuple{};

template<int I, typename IndexTuple, typename... Types>
struct make_indexes_impl;

template<int I, int... Indexes, typename T, typename ... Types>
struct make_indexes_impl<I, index_tuple<Indexes...>, T, Types...>
{
    typedef typename make_indexes_impl<I + 1, index_tuple<Indexes..., I>, Types...>::type type;
};

template<int I, int... Indexes>
struct make_indexes_impl<I, index_tuple<Indexes...> >
{
    typedef index_tuple<Indexes...> type;
};

template<typename ... Types>
struct make_indexes : make_indexes_impl<0, index_tuple<>, Types...>
{};

}

template <
    class Key,
    class T,
    std::size_t Partitions,
    std::size_t(*Partitioner)(Key const&),
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

        partitioned_map_iterator(
                map_type& map,
                std::size_t partition,
                map_iterator iter)
            : map_ (&map)
            , partition_ (partition)
            , it_ (iter)
        {
        }

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

    // Should be private:
    template <
        class... T1,
        class... T2,
        int... I1,
        int... I2>
    auto
    emplace_special (
        std::size_t partition,
        std::tuple<T1...> const& t1,
        std::tuple<T2...> const& t2,
        detail::index_tuple<I1...>,
        detail::index_tuple<I2...>)
    {
        return map_.at(partition).emplace(
            std::piecewise_construct,
            std::tuple<T1...>(std::forward<T1>(std::get<I1>(t1))...),
            std::tuple<T2...>(std::forward<T2>(std::get<I2>(t2))...));
    }

    template <class... T1, class... T2>
    std::pair<iterator, bool>
    emplace (std::piecewise_construct_t, std::tuple<T1...>&& t1, std::tuple<T2...>&& t2)
    {
        auto partition = Partitioner(std::get<0>(t1));

        auto ret = emplace_special (partition,
            std::forward<std::tuple<T1...>>(t1),
            std::forward<std::tuple<T2...>>(t2),
            typename detail::make_indexes<T1...>::type(),
            typename detail::make_indexes<T2...>::type());

        return std::make_pair (
            iterator(map_, partition, ret.first), ret.second);
    }
};

//------------------------------------------------------------------------------

std::size_t
partitioner (uint256 const& key);

} // ripple

#endif // RIPPLE_BASICS_PARTITIONED_MAP_H_INCLUDED
