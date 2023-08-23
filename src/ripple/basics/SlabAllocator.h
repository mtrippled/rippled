//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright 2022, Nikolaos D. Bougalis <nikb@bougalis.net>

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

#ifndef RIPPLE_BASICS_SLABALLOCATOR_H_INCLUDED
#define RIPPLE_BASICS_SLABALLOCATOR_H_INCLUDED

#include <ripple/basics/ByteUtilities.h>
#include <ripple/beast/type_name.h>

#include <boost/align.hpp>
#include <boost/container/static_vector.hpp>
#include <boost/predef.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>

#include <iostream>

#if BOOST_OS_LINUX
#include <sys/mman.h>
#endif

namespace ripple {

template <typename Type>
class SlabAllocator
{
    static_assert(
        sizeof(Type) >= sizeof(std::uint8_t*),
        "SlabAllocator: the requested object must be larger than a pointer.");

    static_assert(alignof(Type) == 16 || alignof(Type) == 8 || alignof(Type) == 4);

    /** A block of memory that is owned by a slab allocator */
    struct SlabBlock
    {
        // A mutex to protect the freelist for this block:
        std::mutex m_;

        // A linked list of appropriately sized free buffers:
        std::uint8_t* l_ = nullptr;

        // The next memory block
        SlabBlock* next_;

        // The underlying memory block:
        std::uint8_t const* const p_ = nullptr;

        // The extent of the underlying memory block:
        std::size_t const size_;

        SlabBlock(
            SlabBlock* next,
            std::uint8_t* data,
            std::size_t size,
            std::size_t item)
            : next_(next), p_(data), size_(size)
        {
            // We don't need to grab the mutex here, since we're the only
            // ones with access at this moment.

            while (data + item <= p_ + size_)
            {
                // Use memcpy to avoid unaligned UB
                // (will optimize to equivalent code)
                std::memcpy(data, &l_, sizeof(std::uint8_t*));
                l_ = data;
                data += item;
            }
        }

        ~SlabBlock()
        {
            // Calling this destructor will release the allocated memory but
            // will not properly destroy any objects that are constructed in
            // the block itself.
        }

        SlabBlock(SlabBlock const& other) = delete;
        SlabBlock&
        operator=(SlabBlock const& other) = delete;

        SlabBlock(SlabBlock&& other) = delete;
        SlabBlock&
        operator=(SlabBlock&& other) = delete;

        /** Determines whether the given pointer belongs to this allocator */
        bool
        own(std::uint8_t const* p) const noexcept
        {
            return (p >= p_) && (p < p_ + size_);
        }

        std::uint8_t*
        allocate(bool debug = false) noexcept
        {
            std::uint8_t* ret;

            {
                std::lock_guard l(m_);

                ret = l_;

                if (ret)
                {
                    // Use memcpy to avoid unaligned UB
                    // (will optimize to equivalent code)
                    std::memcpy(&l_, ret, sizeof(std::uint8_t*));
                }
            }

            return ret;
        }

        /** Return an item to this allocator's freelist.

            @param ptr The pointer to the chunk of memory being deallocated.

            @note This is a dangerous, private interface; the item being
                  returned should belong to this allocator. Debug builds
                  will check and assert if this is not the case. Release
                  builds will not.
         */
        void
        deallocate(std::uint8_t* ptr) noexcept
        {
            assert(own(ptr));

            std::lock_guard l(m_);

            // Use memcpy to avoid unaligned UB
            // (will optimize to equivalent code)
            std::memcpy(ptr, &l_, sizeof(std::uint8_t*));
            l_ = ptr;
        }
    };

private:
    // A linked list of slabs
    std::atomic<SlabBlock*> slabs_ = nullptr;

    // The alignment requirements of the item we're allocating:
    std::size_t const itemAlignment_;

    // The size of an item, including the extra bytes requested and
    // any padding needed for alignment purposes:
    std::size_t const itemSize_;

    // The size of each individual slab:
    std::size_t const slabSize_;

public:
    /** Constructs a slab allocator able to allocate objects of a fixed size

        @param count the number of items the slab allocator can allocate; note
                     that a count of 0 is valid and means that the allocator
                     is, effectively, disabled. This can be very useful in some
                     contexts (e.g. when mimimal memory usage is needed) and
                     allows for graceful failure.
     */
    constexpr explicit SlabAllocator(
        std::size_t extra,
        std::size_t alloc = 0,
        std::size_t align = 0)
        : itemAlignment_(align ? align : alignof(Type))
        , itemSize_(
              boost::alignment::align_up(sizeof(Type) + extra, itemAlignment_))
        , slabSize_(alloc)
    {
        assert((itemAlignment_ & (itemAlignment_ - 1)) == 0);
    }

    SlabAllocator(SlabAllocator const& other) = delete;
    SlabAllocator&
    operator=(SlabAllocator const& other) = delete;

    SlabAllocator(SlabAllocator&& other) = delete;
    SlabAllocator&
    operator=(SlabAllocator&& other) = delete;

    ~SlabAllocator()
    {
        // FIXME: We can't destroy the memory blocks we've allocated, because
        //        we can't be sure that they are not being used. Cleaning the
        //        shutdown process up could make this possible.
    }

    /** Returns the size of the memory block this allocator returns. */
    constexpr std::size_t
    size() const noexcept
    {
        return itemSize_;
    }

    /** Returns a suitably aligned pointer, if one is available.

        @return a pointer to a block of memory from the allocator, or
                nullptr if the allocator can't satisfy this request.
     */
    std::uint8_t*
    allocate() noexcept
    {
        auto slab = slabs_.load();

        while (slab != nullptr)
        {
            if (auto ret = slab->allocate())
                return ret;

            slab = slab->next_;
        }

        // No slab can satisfy our request, so we attempt to allocate a new
        // one here:
        std::size_t size = slabSize_;

        // We want to allocate the memory at a 2 MiB boundary, to make it
        // possible to use hugepage mappings on Linux:
        auto buf =
            boost::alignment::aligned_alloc(megabytes(std::size_t(2)), size);

        // clang-format off
        if (!buf) [[unlikely]]
            return nullptr;
            // clang-format on

#if BOOST_OS_LINUX
        // When allocating large blocks, attempt to leverage Linux's
        // transparent hugepage support. It is unclear and difficult
        // to accurately determine if doing this impacts performance
        // enough to justify using platform-specific tricks.
        if (size >= megabytes(std::size_t(4)))
            madvise(buf, size, MADV_HUGEPAGE);
#endif

        // We need to carve out a bit of memory for the slab header
        // and then align the rest appropriately:
        auto slabData = reinterpret_cast<void*>(
            reinterpret_cast<std::uint8_t*>(buf) + sizeof(SlabBlock));
        auto slabSize = size - sizeof(SlabBlock);

        // This operation is essentially guaranteed not to fail but
        // let's be careful anyways.
        if (!boost::alignment::align(
                itemAlignment_, itemSize_, slabData, slabSize))
        {
            boost::alignment::aligned_free(buf);
            return nullptr;
        }

        slab = new (buf) SlabBlock(
            slabs_.load(),
            reinterpret_cast<std::uint8_t*>(slabData),
            slabSize,
            itemSize_);
        assert(slab);

        // Link the new slab
        while (!slabs_.compare_exchange_weak(
            slab->next_,
            slab,
            std::memory_order_release,
            std::memory_order_relaxed))
        {
            ;  // Nothing to do
        }

        return slab->allocate(true);
    }

    /** Returns the memory block to the allocator.

        @param ptr A pointer to a memory block.
        @param size If non-zero, a hint as to the size of the block.
        @return true if this memory block belonged to the allocator and has
                     been released; false otherwise.
     */
    bool
    deallocate(std::uint8_t* ptr) noexcept
    {
        assert(ptr);

        for (auto slab = slabs_.load(); slab != nullptr; slab = slab->next_)
        {
            if (slab->own(ptr))
            {
                slab->deallocate(ptr);
                return true;
            }
        }

        return false;
    }
};

/** A collection of slab allocators of various sizes for a given type. */
template <typename Type>
class SlabAllocatorSet
{
private:
    // The list of allocators that belong to this set
    boost::container::static_vector<SlabAllocator<Type>, 64> allocators_;

    std::size_t maxSize_ = 0;

public:
    class SlabConfig
    {
        friend class SlabAllocatorSet;

    private:
        std::size_t extra;
        std::size_t alloc;
        std::size_t align;

    public:
        constexpr SlabConfig(
            std::size_t extra_,
            std::size_t alloc_ = 0,
            std::size_t align_ = alignof(Type))
            : extra(extra_), alloc(alloc_), align(align_)
        {
        }
    };

    constexpr SlabAllocatorSet(std::vector<SlabConfig> cfg)
    {
        // Ensure that the specified allocators are sorted from smallest to
        // largest by size:
        std::sort(
            std::begin(cfg),
            std::end(cfg),
            [](SlabConfig const& a, SlabConfig const& b) {
                return a.extra < b.extra;
            });

        // We should never have two slabs of the same size
        if (std::adjacent_find(
                std::begin(cfg),
                std::end(cfg),
                [](SlabConfig const& a, SlabConfig const& b) {
                    return a.extra == b.extra;
                }) != cfg.end())
        {
            throw std::runtime_error(
                "SlabAllocatorSet<" + beast::type_name<Type>() +
                ">: duplicate slab size");
        }

        for (auto const& c : cfg)
        {
            auto& a = allocators_.emplace_back(c.extra, c.alloc, c.align);

            if (a.size() > maxSize_)
                maxSize_ = a.size();
        }
    }

    SlabAllocatorSet(SlabAllocatorSet const& other) = delete;
    SlabAllocatorSet&
    operator=(SlabAllocatorSet const& other) = delete;

    SlabAllocatorSet(SlabAllocatorSet&& other) = delete;
    SlabAllocatorSet&
    operator=(SlabAllocatorSet&& other) = delete;

    ~SlabAllocatorSet()
    {
    }

    /** Returns a suitably aligned pointer, if one is available.

        @param extra The number of extra bytes, above and beyond the size of
                     the object, that should be returned by the allocator.

        @return a pointer to a block of memory, or nullptr if the allocator
                can't satisfy this request.
     */
    std::uint8_t*
    allocate(std::size_t extra) noexcept
    {
        if (auto const size = sizeof(Type) + extra; size <= maxSize_)
        {
            for (auto& a : allocators_)
            {
                if (a.size() >= size)
                    return a.allocate();
            }
        }

        return nullptr;
    }

    /** Returns the memory block to the allocator.

        @param ptr A pointer to a memory block.

        @return true if this memory block belonged to one of the allocators
                     in this set and has been released; false otherwise.
     */
    bool
    deallocate(std::uint8_t* ptr) noexcept
    {
        for (auto& a : allocators_)
        {
            if (a.deallocate(ptr))
                return true;
        }

        return false;
    }
};

// clang-format off
// The slab cutoffs and the number of megabytes per allocation are customized
// based on the number of objects of each size we expect to need at any point
// in time and with an eye to minimize the number of slack bytes in a block.
inline SlabAllocatorSet<std::max_align_t> globalSlabber({
    {  64 },
    {  128 },
    {  512 },
    {  1024 },
    {  8192 },
});
// clang-format on

//-----------------------------------------------------------------------------

template <class T>
class slab_allocator
{
public:
    using value_type    = T;

//     using pointer       = value_type*;
//     using const_pointer = typename std::pointer_traits<pointer>::template
//                                                     rebind<value_type const>;
//     using void_pointer       = typename std::pointer_traits<pointer>::template
//                                                           rebind<void>;
//     using const_void_pointer = typename std::pointer_traits<pointer>::template
//                                                           rebind<const void>;

//     using difference_type = typename std::pointer_traits<pointer>::difference_type;
//     using size_type       = std::make_unsigned_t<difference_type>;

//     template <class U> struct rebind {typedef allocator<U> other;};

    slab_allocator() noexcept {}  // not required, unless used
    template <class U> slab_allocator(slab_allocator<U> const&) noexcept {}

    value_type*  // Use pointer if pointer is not a value_type*
    allocate(std::size_t n)
    {
        std::size_t s = n * sizeof(value_type);
        uint8_t* ret = globalSlabber.allocate(s);
        if (ret != nullptr)
            return reinterpret_cast<value_type*>(ret);
        return static_cast<value_type*>(::operator new (n*sizeof(value_type)));

//            new static_cast<uint_8*>((ret) <uint8_t*>(s));
//        return reinterpret_cast<value_type*>(ret);


//        value_type* ret = reinterpret_cast<value_type*>(
//            globalSlabber.allocate(n * sizeof(value_type)));
        // If we can't grab memory from the slab allocators, we fall back to
        // the standard library and try to grab a precisely-sized memory block:
//        if (ret == nullptr)
//            return new value_type[n];
//        return new (ret) value_type[n];

//            return static_cast<value_type*>(
//                ::operator new (n * sizeof(value_type)));
//        ::operator new (n, ret) sizeof(value_type));
//        return ret;
//        return ::operator new (ret) static_cast<value_type*>(n * sizeof(value_type));

//        return static_cast<value_type*>(::operator new (n*sizeof(value_type)));
    }

    void
    deallocate(value_type* p, std::size_t s) noexcept  // Use pointer if pointer is not a value_type*
    {
        if (p == nullptr)
            return;
        if (!globalSlabber.deallocate(reinterpret_cast<std::uint8_t*>(p)))
            ::operator delete(p);
//            delete p;
//        ::operator delete(p);
    }

//     value_type*
//     allocate(std::size_t n, const_void_pointer)
//     {
//         return allocate(n);
//     }

//     template <class U, class ...Args>
//     void
//     construct(U* p, Args&& ...args)
//     {
//         ::new(p) U(std::forward<Args>(args)...);
//     }

//     template <class U>
//     void
//     destroy(U* p) noexcept
//     {
//         p->~U();
//     }

//     std::size_t
//     max_size() const noexcept
//     {
//         return std::numeric_limits<size_type>::max();
//     }

//     allocator
//     select_on_container_copy_construction() const
//     {
//         return *this;
//     }

//     using propagate_on_container_copy_assignment = std::false_type;
//     using propagate_on_container_move_assignment = std::false_type;
//     using propagate_on_container_swap            = std::false_type;
//     using is_always_equal                        = std::is_empty<allocator>;
};

template <class T, class U>
bool
operator==(slab_allocator<T> const&, slab_allocator<U> const&) noexcept
{
    return true;
}

template <class T, class U>
bool
operator!=(slab_allocator<T> const& x, slab_allocator<U> const& y) noexcept
{
    return !(x == y);
}

}  // namespace ripple

#endif  // RIPPLE_BASICS_SLABALLOCATOR_H_INCLUDED
