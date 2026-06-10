#pragma once
// ----------------------------------------------------------------------------
// Metrics — single global registry.
//
// Hot-path cost: relaxed atomic increments + histogram record (~5-10 ns).
// A cold metrics thread (1 Hz) snapshots everything and publishes to:
//   * prometheus-cpp Exposer  ->  scraped by Prometheus, graphed in Grafana
//   * stdout one-line summary ->  quick eyeballing without the stack
//
// Two critical flows measured (as required by the assignment):
//   F1  md_processing : fromApp() entry -> order book updated (strategy)
//   F2  tick_to_trade : fromApp() entry -> NewOrderSingle handed to socket
// Plus wire_to_strategy (queue latency) to expose backlog-induced delay.
// ----------------------------------------------------------------------------
#include "../common/latency_histogram.hpp"
#include <atomic>
#include <cstdint>

struct Metrics {
    // latency flows
    LatencyHistogram md_processing;     // F1
    LatencyHistogram tick_to_trade;     // F2
    LatencyHistogram queue_wait;        // MD ring residency time

    // throughput
    std::atomic<uint64_t> md_msgs_total{0};
    std::atomic<uint64_t> md_dropped_total{0};
    std::atomic<uint64_t> orders_sent_total{0};
    std::atomic<uint64_t> cancels_sent_total{0};
    std::atomic<uint64_t> execs_total{0};

    // backlog high-water marks (assignment: max size of internal queues)
    std::atomic<uint64_t> md_queue_max{0};
    std::atomic<uint64_t> ord_queue_max{0};
    std::atomic<uint64_t> log_queue_max{0};

    void track_max(std::atomic<uint64_t>& m, uint64_t v) noexcept {
        uint64_t cur = m.load(std::memory_order_relaxed);
        while (v > cur && !m.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {}
    }

    static Metrics& instance() { static Metrics m; return m; }
};
