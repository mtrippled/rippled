//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

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

#ifndef RIPPLE_LEDGER_RAWSTATETABLE_H_INCLUDED
#define RIPPLE_LEDGER_RAWSTATETABLE_H_INCLUDED

#include <ripple/ledger/RawView.h>
#include <ripple/ledger/ReadView.h>
#include <ripple/basics/qalloc.h>
#include <map>
#include <utility>

namespace ripple {
namespace detail {

// parallel map
template < class Key,
           class T,
           std::size_t Partitions,
           class Partitioner,
           class Compare = std::less<Key>,
           class Alloc = std::allocator<std::pair<const Key,T> >
         >
class parallel_map_iter;

template < class Key,
           class T,
           std::size_t Partitions,
           class Partitioner,
           class Compare = std::less<Key>,
           class Alloc = std::allocator<std::pair<const Key,T> >
         >
class parallel_map
{
    friend parallel_map_iter< Key, T, Partitions, Partitioner, Compare, Alloc >;

    std::array<std::map<Key, T, Compare, Alloc>, Partitions> map_;

public:
    using key_type        = Key;
    using mapped_type     = T;
    using value_type      = std::pair<const key_type, mapped_type>;
    using key_compare     = Compare;
    // using value_compare = ?;
    using allocator_type  = Alloc;
    using reference       = value_type&;
    using const_reference = value_type const&;
    using pointer         = value_type*;
    using iterator        = parallel_map_iter<Key, T, Partitions, Partitioner,
        Compare, Alloc>;
    using const_iterator  = parallel_map_iter<Key, T, Partitions, Partitioner,
        Compare, Alloc>;
    // using reverse_iterator = ?;
    // using const_reverse_iterator = ?;
    using difference_type = std::ptrdiff_t;
    using size_type       = std::size_t;

    iterator
    begin()
    {
        return iterator(map_).begin();
    }

    iterator
    end()
    {
        return iterator(map_).end();
    }

    iterator
    find (key_type const& k)
    {
        return iterator(map_).find(k);
    }

    iterator
    upper_bound (key_type const& k)
    {
        return iterator(map_).upper_bound(k);
    }

    void
    erase (iterator it)
    {
        iterator(map_).erase(it);
    }
};

template < class Key,
           class T,
           std::size_t Partitions,
           class Partitioner,
           class Compare,
           class Alloc
         >
class parallel_map_iter
{
    using key_type        = Key;
    using mapped_type     = T;
    using value_type      = std::pair<const key_type, mapped_type>;
    using iterator = parallel_map_iter<Key, T, Partitions, Partitioner, Compare,
        Alloc>;

    parallel_map<Key, T, Partitions, Partitioner, Compare, Alloc>& map_;
    std::size_t partition_ = 0;
    typename std::map<Key, T, Compare, Alloc>::iterator it_;

    iterator&
    inc()
    {
        ++it_;

        while (it_ == map_[partition_].end())
        {
            if (partition_ == Partitions - 1)
                return *this;

            it_ = map_[++partition_].begin();
        }

        return *this;
    }

public:
    parallel_map_iter(parallel_map<Key, T, Partitions, Partitioner, Compare,
        Alloc>& map)
        : map_ (map)
    {}

    iterator&
    begin()
    {
        partition_ = 0;
        it_ = map_[partition_].begin();
        return *this;
    }

    iterator&
    end()
    {
        partition_ = Partitions - 1;
        it_ = map_[partition_].end();
        return *this;
    }

    iterator&
    find (key_type const& k)
    {
        partition_ = Partitioner(k);
        it_ = map_[partition_].find(k);
        return *this;
    }

    iterator&
    upper_bound (key_type const& k)
    {
        partition_ = Partitioner(k);
        it_ = map_[partition_].upper_bound(k);

        while (it_ == map_[partition_].end())
        {
            if (partition_ == Partitions - 1)
                return end();

            it_ = map_[++partition_].begin();
        }

        return *this;
    }

    void
    erase (iterator it)
    {
        map_[partition_].erase(it);
    }

    value_type&
    operator*()
    {
        return *it_;
    }

    bool
    operator== (iterator const& other)
    {
        return (partition_ == other.partition_) && (it_ == other.it_);
    }

    bool
    operator!= (iterator const& other)
    {
        return ! (*this == other);
    }

    iterator&
    operator++()
    {
        return inc();
    }

    iterator&
    operator++ (int)
    {
        iterator tmp(*this);
        inc();
        return tmp;
    }
};

std::size_t
partitioner (uint256 const& k)
{
    return 42;
}

// Helper class that buffers raw modifications
class RawStateTable
{
public:
    using key_type = ReadView::key_type;

    RawStateTable() = default;
    RawStateTable (RawStateTable const&) = default;
    RawStateTable (RawStateTable&&) = default;

    RawStateTable& operator= (RawStateTable&&) = delete;
    RawStateTable& operator= (RawStateTable const&) = delete;

    void
    apply (RawView& to) const;

    bool
    exists (ReadView const& base,
        Keylet const& k) const;

    boost::optional<key_type>
    succ (ReadView const& base,
        key_type const& key, boost::optional<
            key_type> const& last) const;

    void
    erase (std::shared_ptr<SLE> const& sle);

    void
    insert (std::shared_ptr<SLE> const& sle);

    void
    replace (std::shared_ptr<SLE> const& sle);

    std::shared_ptr<SLE const>
    read (ReadView const& base,
        Keylet const& k) const;

    void
    destroyXRP (XRPAmount const& fee);

    std::unique_ptr<ReadView::sles_type::iter_base>
    slesBegin (ReadView const& base) const;

    std::unique_ptr<ReadView::sles_type::iter_base>
    slesEnd (ReadView const& base) const;

    std::unique_ptr<ReadView::sles_type::iter_base>
    slesUpperBound (ReadView const& base, uint256 const& key) const;

private:
    enum class Action
    {
        erase,
        insert,
        replace,
    };

    class sles_iter_impl;

    /*
    using items_t = std::map<key_type,
        std::pair<Action, std::shared_ptr<SLE>>,
        std::less<key_type>, qalloc_type<std::pair<key_type const,
        std::pair<Action, std::shared_ptr<SLE>>>, false>>;
     */
    using items_t = detail::parallel_map<key_type,
        std::pair<Action, std::shared_ptr<SLE>>,
        16,
        std::function<std::size_t (uint256 const&)>,
        std::less<key_type>, qalloc_type<std::pair<key_type const,
        std::pair<Action, std::shared_ptr<SLE>>>, false>>;

    items_t items_;
    XRPAmount dropsDestroyed_ = 0;
};

} // detail
} // ripple

#endif
