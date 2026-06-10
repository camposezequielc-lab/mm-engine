#pragma once
// ----------------------------------------------------------------------------
// SPSC lock-free ring buffer.
//
// Design notes:
//  - Capacity is a power of two -> index wrap is a bitwise AND, no modulo.
//  - head/tail are on separate cache lines (alignas(64)) to avoid false
//    sharing between the producer and consumer cores.
//  - Each side keeps a *cached* copy of the opposite index, so the common
//    case touches only its own cache line (no cross-core traffic until the
//    cached value says "maybe full/empty").
//  - Elements are trivially copyable PODs. No allocation ever happens here:
//    the buffer is a fixed array, so the hot path is allocation-free.
//  - Memory order: release on publish, acquire on observe. No seq_cst.
// ----------------------------------------------------------------------------
#include <atomic>
#include <cstddef>
#include <type_traits>
#include <new>
#include <cstdint>

#if defined(__cpp_lib_hardware_interference_size)
inline constexpr std::size_t kCacheLine = std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t kCacheLine = 64;
#endif

template <typename T, std::size_t CapacityPow2>
class SpscRing {
    static_assert((CapacityPow2 & (CapacityPow2 - 1)) == 0,
                  "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>,
                  "T must be trivially copyable (POD-style) for a lock-free ring");

public:
    SpscRing() : head_(0), tail_(0), cached_head_(0), cached_tail_(0) {}

    // Producer side. Returns false if full (caller decides: drop / spin / count).
    bool try_push(const T& item) noexcept {
        const auto head = head_.load(std::memory_order_relaxed);
        const auto next = head + 1;
        if (next - cached_tail_ > CapacityPow2) {              // looks full?
            cached_tail_ = tail_.load(std::memory_order_acquire);
            if (next - cached_tail_ > CapacityPow2) return false;
        }
        buf_[head & kMask] = item;
        head_.store(next, std::memory_order_release);          // publish
        return true;
    }

    // Consumer side. Returns false if empty.
    bool try_pop(T& out) noexcept {
        const auto tail = tail_.load(std::memory_order_relaxed);
        if (tail == cached_head_) {                            // looks empty?
            cached_head_ = head_.load(std::memory_order_acquire);
            if (tail == cached_head_) return false;
        }
        out = buf_[tail & kMask];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Approximate occupancy,used by the metrics thread for backlog gauges.
    std::size_t size_approx() const noexcept {
        const auto h = head_.load(std::memory_order_acquire);
        const auto t = tail_.load(std::memory_order_acquire);
        return static_cast<std::size_t>(h - t);
    }

    static constexpr std::size_t capacity() noexcept { return CapacityPow2; }

private:
    static constexpr std::size_t kMask = CapacityPow2 - 1;

    alignas(kCacheLine) std::atomic<std::uint64_t> head_;   // producer writes
    alignas(kCacheLine) std::atomic<std::uint64_t> tail_;   // consumer writes
    alignas(kCacheLine) std::uint64_t cached_head_;          // consumer-local
    alignas(kCacheLine) std::uint64_t cached_tail_;          // producer-local
    alignas(kCacheLine) T buf_[CapacityPow2];
};
