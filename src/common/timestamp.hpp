#pragma once
// ----------------------------------------------------------------------------
// Nanosecond timestamping.
//
// Two clocks:
//   - now_ns()  -> std::chrono::steady_clock, portable, ~20-30 ns per call.
//   - rdtsc()   -> raw TSC read, ~7-10 ns, monotonic on modern CPUs with
//                  invariant TSC (constant_tsc flag on Linux). We calibrate
//                  ticks-per-ns once at startup so deltas can be converted.
//
// All latency math in the engine is done on int64 nanosecond deltas.
// ----------------------------------------------------------------------------
#include <chrono>
#include <cstdint>

#if defined(_MSC_VER)
  #include <intrin.h>
#elif defined(__x86_64__) || defined(__i386__)
  #include <x86intrin.h>
#endif

namespace ts {

inline int64_t now_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

inline uint64_t rdtsc() noexcept {
#if defined(_MSC_VER) || defined(__x86_64__) || defined(__i386__)
    return __rdtsc();
#else
    return static_cast<uint64_t>(now_ns());
#endif
}

// Calibrate TSC frequency against steady_clock (call once at startup).
struct TscCal {
    double ticks_per_ns = 1.0;
    void calibrate() {
        const auto t0n = now_ns(); const auto t0t = rdtsc();
        // Busy-wait ~50ms; coarse but plenty for percentile work.
        while (now_ns() - t0n < 50'000'000) {}
        const auto t1n = now_ns(); const auto t1t = rdtsc();
        ticks_per_ns = double(t1t - t0t) / double(t1n - t0n);
        if (ticks_per_ns <= 0) ticks_per_ns = 1.0;
    }
    int64_t to_ns(uint64_t ticks) const noexcept {
        return static_cast<int64_t>(double(ticks) / ticks_per_ns);
    }
};

inline TscCal& cal() { static TscCal c; return c; }

} // namespace ts
