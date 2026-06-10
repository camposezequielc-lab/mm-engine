#pragma once
// ----------------------------------------------------------------------------
// OrderBook — per-instrument L2 book, fixed depth (<=5 per ROFEX spec).
//
// Why arrays and not std::map<price, qty>:
//  - Depth is tiny and bounded; two flat arrays per side fit in ONE cache line
//    each (5 doubles = 40 bytes). A full snapshot replace is a straight copy.
//  - ROFEX MD here is Snapshot/Full Refresh (MDUpdateType=0): every W message
//    replaces the book, so I don't need incremental insert/delete — the
//    optimal structure is literally memcpy-style assignment.
//  - BBA is bid_px[0]/ask_px[0]: O(1), zero pointer chasing.
//
// SIMD: with depth<=5 the win is small, but I vectorize the depth-weighted
// VWAP (sum px*qty and sum qty across levels) with AVX2 when available and fall back to scalar elsewhere.
// ----------------------------------------------------------------------------
#include "../common/events.hpp"
#include <cstring>

#if defined(__AVX2__)
  #include <immintrin.h>
#endif

struct alignas(64) OrderBook {
    double bid_px[kMaxDepth] = {};
    double bid_qty[kMaxDepth] = {};
    double ask_px[kMaxDepth] = {};
    double ask_qty[kMaxDepth] = {};
    uint16_t n_bids = 0, n_asks = 0;
    int64_t  last_update_ns = 0;
    uint32_t seq = 0;

    void apply_snapshot(const MdEvent& e, int max_depth) noexcept {
        n_bids = e.n_bids > max_depth ? uint16_t(max_depth) : e.n_bids;
        n_asks = e.n_asks > max_depth ? uint16_t(max_depth) : e.n_asks;
        std::memcpy(bid_px,  e.bid_px,  sizeof(bid_px));
        std::memcpy(bid_qty, e.bid_qty, sizeof(bid_qty));
        std::memcpy(ask_px,  e.ask_px,  sizeof(ask_px));
        std::memcpy(ask_qty, e.ask_qty, sizeof(ask_qty));
        last_update_ns = e.t_recv_ns;
        seq = e.seq;
    }

    bool has_bba() const noexcept { return n_bids > 0 && n_asks > 0; }
    double best_bid() const noexcept { return bid_px[0]; }
    double best_ask() const noexcept { return ask_px[0]; }
    double spread()   const noexcept { return ask_px[0] - bid_px[0]; }
    double mid()      const noexcept { return 0.5 * (ask_px[0] + bid_px[0]); }

    // Depth-weighted theoretical price (simple VWAP over visible levels).
    double vwap() const noexcept {
#if defined(__AVX2__)
        // process 4 levels at a time: sum(px*qty), sum(qty) for both sides
        __m256d acc_pq = _mm256_setzero_pd();
        __m256d acc_q  = _mm256_setzero_pd();
        // bids (only first 4 lanes; level 5 handled scalar below)
        __m256d bpx = _mm256_loadu_pd(bid_px);
        __m256d bqt = _mm256_loadu_pd(bid_qty);
        __m256d apx = _mm256_loadu_pd(ask_px);
        __m256d aqt = _mm256_loadu_pd(ask_qty);
        acc_pq = _mm256_fmadd_pd(bpx, bqt, acc_pq);
        acc_pq = _mm256_fmadd_pd(apx, aqt, acc_pq);
        acc_q  = _mm256_add_pd(acc_q, _mm256_add_pd(bqt, aqt));
        alignas(32) double pq[4], q[4];
        _mm256_store_pd(pq, acc_pq);
        _mm256_store_pd(q,  acc_q);
        double sum_pq = pq[0]+pq[1]+pq[2]+pq[3]
                      + bid_px[4]*bid_qty[4] + ask_px[4]*ask_qty[4];
        double sum_q  = q[0]+q[1]+q[2]+q[3] + bid_qty[4] + ask_qty[4];
#else
        double sum_pq = 0, sum_q = 0;
        for (int i = 0; i < kMaxDepth; ++i) {
            sum_pq += bid_px[i]*bid_qty[i] + ask_px[i]*ask_qty[i];
            sum_q  += bid_qty[i] + ask_qty[i];
        }
#endif
        return sum_q > 0 ? sum_pq / sum_q : mid();
    }
};
