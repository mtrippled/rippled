//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2015 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_ATOMICARRAY_H_INCLUDED
#define RIPPLE_BASICS_ATOMICARRAY_H_INCLUDED

#include <ripple/json/json_value.h>
#include <atomic>
#include <memory>
#include <sstream>
#include <chrono>
#include <string>
#include <vector>
#include <cassert>
#include <algorithm>

namespace ripple {

namespace perf_jss {
    // JSON static strings.
    const Json::StaticString time ("time");
    const Json::StaticString hostname ("hostname");
    const Json::StaticString pid ("pid");
    const Json::StaticString entries ("entries");
    const Json::StaticString ter_counters ("ter_counters");
    const Json::StaticString job_queue_counters ("job_queue_counters");
    const Json::StaticString node_store_counters ("node_store_counters");
    const Json::StaticString num_workers ("num_workers");
    const Json::StaticString trace ("trace");
    const Json::StaticString type ("type");
    const Json::StaticString name ("name");
    const Json::StaticString tid ("tid");
    const Json::StaticString counter ("counter");
    const Json::StaticString events ("events");
    const Json::StaticString start_time ("start_time");
    const Json::StaticString end ("end_time");
    const Json::StaticString rpc_command ("rpc_command");
    const Json::StaticString rpc_counter ("rpc_counter");
    const Json::StaticString duration_us ("duration_us");
    const Json::StaticString starting ("starting");
}

/**
 * This works around std::atomic not working with containers.
 */
template <class T>
class AtomicCounter
{
public:
    AtomicCounter() = default;

    /**
     * Work around atomics being non-copyable. Don't allow values to be
     * lost when copying into a container.
     *
     * @param orig
     */
    AtomicCounter (AtomicCounter const& orig)
    {
        assert (! orig.val_);
    }

    AtomicCounter (AtomicCounter const&& orig)
    {
        assert (! orig.val_);
    }

    AtomicCounter
    operator= (AtomicCounter const& orig) const
    {
        assert (! orig.val_);
        return *this;
    }

    T add (T const& val=0)
    {
        return val_ += val;
    }

    T set (T const& val)
    {
        return val_ = val;
    }

    T get() const
    {
        return val_;
    }

private:
    std::atomic<T> val_ {0};
};

template <class T>
class AtomicArray
{
public:
    AtomicArray (std::size_t const size,
            Json::StaticString const name="")
        : name_ (name)
    {
        vector_.resize (size);
    }

    /**
     * Get array size.
     *
     * @return Number of array elements.
     */
    std::size_t size() const
    {
        return vector_.size();
    }

    /**
     * Get a value from the array.
     *
     * @param pos Index position to retrieve.
     * @return Value at index position.
     */
    T at (std::size_t pos) const
    {
        return vector_[std::min (pos, vector_.size())].get();
    }

    /**
     * Add to a value within the array.
     *
     * @param pos Index position to modify.
     * @param val Amount to add. Defaults to 1.
     * @return The value prior to the arithmetic operation.
     */
    T add (std::size_t pos, T val=1)
    {
        return vector_[std::min (pos, vector_.size())].add (val);
    }

    /** Return the JSON field name. */
    Json::StaticString const& name() const
    {
        return name_;
    }

    /**
     * Convert the array to JSON.
     *
     * @return JSON array.
     */
    Json::Value json() const
    {
        Json::Value ret (Json::arrayValue);
        for (auto const& e : vector_)
            ret.append (std::to_string (e.get()));
        return ret;
    }

private:
    Json::StaticString const name_;
    std::vector <AtomicCounter <T>> vector_;
};

} // ripple

#endif
