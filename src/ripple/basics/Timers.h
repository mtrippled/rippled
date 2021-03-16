//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2018 Ripple Labs Inc.

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

#ifndef RIPPLE_BASICS_TIMERS_H
#define RIPPLE_BASICS_TIMERS_H

#include <ripple/json/json_value.h>
#include <ripple/protocol/jss.h>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace ripple {
namespace perf {

struct Timers
{
    struct Timer
    {
        struct Tag
        {
            std::string_view label;
            std::string_view mutex_label;
            std::uint64_t mutex_id{0};

            Tag() = default;

            Tag(std::string_view const& labelArg) : label(labelArg)
            {
            }

            Tag(std::string_view const& labelArg,
                std::string_view const& mutexLabelArg,
                std::uint64_t mutexIdArg)
                : label(labelArg)
                , mutex_label(mutexLabelArg)
                , mutex_id(mutexIdArg)
            {
            }

            bool
            operator<(Tag const& other) const
            {
                if (mutex_id)
                {
                    if (label < other.label)
                        return true;
                    if (mutex_label < other.mutex_label)
                        return true;
                    return mutex_id < other.mutex_id;
                }
                else
                {
                    return label < other.label;
                }
            }

            bool
            operator==(Tag const& other) const
            {
                return other.label == label &&
                    other.mutex_label == mutex_label &&
                    other.mutex_id == mutex_id;
            }

            std::string
            tracer() const
            {
                return std::string(label);
            }

            std::string
            subTracer() const
            {
                std::string ret(label);
                if (mutex_id)
                {
                    ret += "-" + std::string(mutex_label) + "." +
                        std::to_string(mutex_id);
                }
                return ret;
            }

            std::string
            mutex() const
            {
                std::string ret(mutex_label);
                ret += "." + std::to_string(mutex_id);
                return ret;
            }

            std::string
            subMutex() const
            {
                return std::string(label);
            }

            void
            toJson(Json::Value& obj) const
            {
                obj[jss::label] = std::string(label);
                obj[jss::mutex_label] = std::string(mutex_label);
                obj[jss::mutex_id] = std::to_string(mutex_id);
            }

            std::string
            mutexStr() const
            {
                return std::string(mutex_label) + "." +
                    std::to_string(mutex_id);
            }
        };

        std::chrono::steady_clock::time_point start_time;
        Tag tag;
        std::chrono::microseconds duration_us;

        Timer(
            std::chrono::steady_clock::time_point const& startTimeArg,
            std::string_view const& labelArg,
            std::string_view const& mutexLabelArg,
            std::uint64_t const mutexIdArg,
            std::chrono::microseconds const& durationUsArg)
            : start_time(startTimeArg)
            , tag({labelArg, mutexLabelArg, mutexIdArg})
            , duration_us(durationUsArg)
        {
        }

        Json::Value
        toJson() const
        {
            Json::Value ret{Json::objectValue};
            ret[jss::start_time_us] = std::to_string(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    start_time.time_since_epoch())
                    .count());
            ret[jss::duration_us] = std::to_string(duration_us.count());
            ret[jss::label] = std::string(tag.label).c_str();
            if (tag.mutex_label.size())
            {
                ret[jss::mutex] = std::string(tag.mutex_label) + "." +
                    std::to_string(tag.mutex_id);
            }

            return ret;
        }
    };

    Timer timer;
    bool render;
    std::vector<Timer> sub_timers;

    Timers(
        std::chrono::steady_clock::time_point const& startTimeArg,
        std::string_view const& labelArg,
        std::string_view const& mutexLabelArg,
        std::uint64_t const mutexIdArg,
        std::chrono::microseconds const& durationUsArg,
        bool const renderArg,
        std::vector<Timer> const& subTimersArg)
        : timer(
              startTimeArg,
              labelArg,
              mutexLabelArg,
              mutexIdArg,
              durationUsArg)
        , render(renderArg)
        , sub_timers(subTimersArg)
    {}
};

}  // namespace perf
}  // namespace ripple

template <>
struct std::hash<ripple::perf::Timers::Timer::Tag>
    {
    std::size_t
    operator()(ripple::perf::Timers::Timer::Tag const& tag) const
    {
        if (tag.mutex_label.empty())
        {
            return std::hash<std::string_view>()(tag.label);
        }
        else
        {
            return std::hash<std::string_view>()(tag.label) +
            std::hash<std::string_view>()(tag.mutex_label) +
            tag.mutex_id;
        }
    }
    };

#endif  // RIPPLED_BASICS_TIMERS_H
