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

#include <ripple/basics/base_uint.h>
#include <ripple/basics/partitioned_unordered_map.h>
#include <ripple/shamap/SHAMapTreeNode.h>

namespace ripple {

std::size_t
extract(uint256 const& key)
{
    return *reinterpret_cast<std::size_t const*>(key.data());
}

std::size_t
extract(SHAMapHash const& key)
{
    return *reinterpret_cast<std::size_t const*>(key.as_uint256().data());
}

std::size_t
extract(std::uint64_t const key)
{
    return static_cast<std::size_t const>(key);
}

template <
    typename Key,
    typename Value,
    typename Hash,
    typename Pred,
    typename Alloc>
std::size_t
partitioned_unordered_map<Key, Value, Hash, Pred, Alloc>::partitioner(
    key_type const& key) const
{
    return extract(key) % partitions_;
}

}  // namespace ripple
