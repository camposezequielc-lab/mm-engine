#pragma once
// ----------------------------------------------------------------------------
// LatencyHistogram — HdrHistogram-style, lock-free, fixed memory.
//
// Recording (hot path):   1 CLZ + 1 relaxed atomic fetch_add  (~3-6 ns).
// Reading  (cold path):   metrics thread walks buckets, computes percentiles.
//
// Layout: 64 "major" buckets (one per power of two of nanoseconds) x 32
// ----------------------------------------------------------------------------
#include <atomic>
#include <array>
#include <cstdint>

#if defined(_MSC_VER)
  #include <intrin.h>
#endif

class LatencyHistogram {
    static constexpr int kMajor = 64;
    static constexpr int kSub   = 32;   // power of two
    static constexpr int kSubBits = 5;

    static inline int log2_floor(uint64_t v) noexcept {
#if defined(_MSC_VER)
        unsigned long idx; _BitScanReverse64(&idx, v | 1); return int(idx);
#else
        return 63 - __builtin_clzll(v | 1);
#endif
    }

public:
    void record(int64_t ns) noexcept {
        if (ns < 0) ns = 0;
        const uint64_t v = uint64_t(ns) | 1;
        const int maj = log2_floor(v);
        // take top kSubBits bits below the leading one as the sub-bucket
        const int sub = maj >= kSubBits
            ? int((v >> (maj - kSubBits)) & (kSub - 1))
            : int(v & (kSub - 1));
        buckets_[maj * kSub + sub].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        // track max (relaxed CAS loop, rare in practice)
        int64_t cur = max_.load(std::memory_order_relaxed);
        while (ns > cur && !max_.compare_exchange_weak(cur, ns, std::memory_order_relaxed)) {}
    }

    struct Percentiles { int64_t p50, p90, p99, max; uint64_t count; };

    // Non-destructive read; called from the metrics thread.
    Percentiles snapshot() const noexcept {
        std::array<uint64_t, kMajor * kSub> local{};
        uint64_t total = 0;
        for (int i = 0; i < kMajor * kSub; ++i) {
            local[i] = buckets_[i].load(std::memory_order_relaxed);
            total += local[i];
        }
        Percentiles out{0,0,0, max_.load(std::memory_order_relaxed), total};
        if (total == 0) return out;
        const uint64_t t50 = (total * 50 + 99) / 100;
        const uint64_t t90 = (total * 90 + 99) / 100;
        const uint64_t t99 = (total * 99 + 99) / 100;
        uint64_t cum = 0;
        for (int i = 0; i < kMajor * kSub; ++i) {
            cum += local[i];
            const int64_t v = bucket_upper(i);
            if (!out.p50 && cum >= t50) out.p50 = v;
            if (!out.p90 && cum >= t90) out.p90 = v;
            if (!out.p99 && cum >= t99) { out.p99 = v; break; }
        }
        return out;
    }

    void reset() noexcept {
        for (auto& b : buckets_) b.store(0, std::memory_order_relaxed);
        count_.store(0, std::memory_order_relaxed);
        max_.store(0, std::memory_order_relaxed);
    }

private:
    static int64_t bucket_upper(int idx) noexcept {
        const int maj = idx / kSub, sub = idx % kSub;
        if (maj < kSubBits) return (int64_t(1) << maj) + sub;
        const int shift = maj - kSubBits;
        return ((int64_t(1) << kSubBits) + sub + 1) << shift;
    }

    std::array<std::atomic<uint64_t>, kMajor * kSub> buckets_{};
    std::atomic<uint64_t> count_{0};
    std::atomic<int64_t>  max_{0};
};
