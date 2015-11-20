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
}

/**
 * This partially works around std::atomic not working with real containers.
 */
template <class T>
class AtomicArray
{
public:
    AtomicArray (std::size_t size,
            Json::StaticString name="")
        : size_ (size)
        , name_ (name)
    {
        array_ = std::unique_ptr<std::atomic<T>[]> (
            new std::atomic<T>[size_]{});
    }

    /**
     * Get array size.
     *
     * @return Number of array elements.
     */
    std::size_t size()
    {
        return size_;
    }

    /**
     * Get pointer to beginning of array.
     *
     * @return Pointer to beginning of array.
     */
    std::atomic<T>* array()
    {
        return array_.get();
    }

    /**
     * Get a value from the array.
     *
     * @param pos Index position to retrieve.
     * @return Value at index position.
     */
    T at (std::size_t pos)
    {
        return (array_.get() + pos)->load();
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
        return (array_.get() + pos)->fetch_add (val);
    }

    /** Return the JSON field name. */
    Json::StaticString& name()
    {
        return name_;
    }

    /**
     * Convert the array to JSON.
     *
     * @return JSON array.
     */
    Json::Value json()
    {
        Json::Value ret (Json::arrayValue);

        for (std::size_t i=0; i < size_; ++i)
        {
            ret.append (std::to_string (at (i)));
        }

        return ret;
    }

private:
    std::size_t size_;
    Json::StaticString name_;
    std::unique_ptr<std::atomic<T>[]> array_;
};

} // ripple

#endif
