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

#include <iterator>
#include <map>

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
    template <bool IsConst>
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

    public:
        parallel_map_iterator (parallel_map<Key, T, Partitions, Partitioner, Compare, Alloc>& map)
            : map_ (&map)
        {}

        parallel_map_iterator (parallel_map_iterator<false> const& other)
            : map_ (other.map_)
            , partition_ (other.partition_)
            , it_ (other.it_)
        {}

        parallel_map_iterator (parallel_map_iterator<true> const& other)
            : map_ (other.map_)
            , partition_ (other.partition_)
            , it_ (other.it_)
        {}

        parallel_map_iterator&
        operator= (parallel_map_iterator const& other) = default;

        bool
        operator== (parallel_map_iterator const& other)
        {
            return (partition_ == other.partition_) && (map_ == other.map_) &&
                (it_ == other.it_);
        }

        bool
        operator+= (parallel_map_iterator const& other)
        {
            return ! (*this == other);
        }
    };

public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type const, mapped_type>;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using difference_type = std::ptrdiff_t;
    using iterator = parallel_map_iterator<false>;
    using const_iterator = parallel_map_iterator<true>;
};

} // ripple

#endif // RIPPLE_BASICS_PARALLEL_MAP_H_INCLUDED
