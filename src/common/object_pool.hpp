#pragma once
// ----------------------------------------------------------------------------
// Fixed-size object pool. All memory is reserved up-front; acquire/release is
// a free-list pop/push, O(1), no heap traffic on the hot path. Single-threaded
// by design (each pipeline stage owns its own pool), which keeps it branch-
// light and lock-free without atomics.
// ----------------------------------------------------------------------------
#include <array>
#include <cstdint>
#include <cassert>

template <typename T, std::size_t N>
class ObjectPool {
public:
    ObjectPool() {
        for (std::size_t i = 0; i < N; ++i) free_[i] = static_cast<uint32_t>(N - 1 - i);
        top_ = N;
    }
    T* acquire() noexcept {
        if (top_ == 0) return nullptr;          // pool exhausted -> caller decides
        return &storage_[free_[--top_]];
    }
    void release(T* p) noexcept {
        const auto idx = static_cast<uint32_t>(p - storage_.data());
        assert(idx < N);
        free_[top_++] = idx;
    }
    std::size_t available() const noexcept { return top_; }

private:
    std::array<T, N>        storage_{};
    std::array<uint32_t, N> free_{};
    std::size_t             top_ = 0;
};
