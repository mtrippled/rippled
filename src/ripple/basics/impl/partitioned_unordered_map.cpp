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

#include <ripple/basics/partitioned_unordered_map.h>

namespace ripple {

/*
template<typename Key,
    typename Value,
    std::uint8_t PartitionBits,
    typename Hash,
    typename Pred,
    typename Alloc>
partitioned_unordered_map<Key, Value, PartitionBits, Hash, Pred, Alloc>::partitioned_unordered_map()
{
    partitioned_unordered_map::iterator it;
    partitioned_unordered_map::const_iterator cit;
    ++it;
    ++cit;
    it++;
    cit++;
    partitioned_unordered_map::iterator it2;
    partitioned_unordered_map::const_iterator cit2;
    if (it == it2)
    {}
    if (it != it2)
    {}
    if (cit == cit2)
    {}
    if (cit != cit2)
    {}
    if (it == cit)
    {}
    if (it != cit)
    {}
    if (cit == it)
    {}
    if (cit != it)
    {}

    it = it2;
    cit = cit2;
    partitioned_unordered_map::iterator it3(it);
    partitioned_unordered_map::const_iterator cit3(cit);

    partitioned_unordered_map::const_iterator cit4(it);
    cit3 = it;
    // shouldn't work (const to non-const)
//        partitioned_unordered_map::iterator it4(cit);
//        it = cit;

    typename map_type::iterator mit;
    typename map_type::iterator mit2;
    if (mit == mit2) {}
    typename map_type::const_iterator cmit;
    typename map_type::const_iterator cmit2;
    if (cmit == cmit2) {}

    it = begin();
    cit = begin();
    it = end();
    cit = end();
}
*/


}  // namespace ripple
