#pragma once
// ----------------------------------------------------------------------------
// MetricsServer — cold-path thread. Owns the prometheus-cpp Exposer and
// publishes gauges/counters once per second. Nothing here touches the hot path
// except relaxed atomic loads.
// ----------------------------------------------------------------------------
#include "metrics.hpp"
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/gauge.h>
#include <prometheus/counter.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>

class MetricsServer {
public:
    explicit MetricsServer(int port)
        : exposer_("0.0.0.0:" + std::to_string(port)),
          registry_(std::make_shared<prometheus::Registry>()) {

        auto& lat = prometheus::BuildGauge()
            .Name("mm_latency_ns").Help("Latency percentiles (ns)").Register(*registry_);
        for (const char* flow : {"md_processing", "tick_to_trade", "queue_wait"})
            for (const char* p : {"p50", "p90", "p99", "max"})
                gauges_[std::string(flow) + ":" + p] =
                    &lat.Add({{"flow", flow}, {"q", p}});

        auto& thr = prometheus::BuildGauge()
            .Name("mm_throughput_msgs_per_s").Help("MD messages processed per second").Register(*registry_);
        tps_ = &thr.Add({});

        auto& cnt = prometheus::BuildCounter()
            .Name("mm_events_total").Help("Engine event counters").Register(*registry_);
        c_md_     = &cnt.Add({{"type", "md"}});
        c_drop_   = &cnt.Add({{"type", "md_dropped"}});
        c_orders_ = &cnt.Add({{"type", "orders"}});
        c_cxl_    = &cnt.Add({{"type", "cancels"}});
        c_exec_   = &cnt.Add({{"type", "execs"}});

        auto& bk = prometheus::BuildGauge()
            .Name("mm_queue_backlog_max").Help("High-water mark of internal queues").Register(*registry_);
        g_mdq_  = &bk.Add({{"queue", "md_in"}});
        g_ordq_ = &bk.Add({{"queue", "order_out"}});
        g_logq_ = &bk.Add({{"queue", "log"}});

        exposer_.RegisterCollectable(registry_);
    }

    void run(std::atomic<bool>& running) {
        auto& m = Metrics::instance();
        uint64_t last_md = 0;
        while (running.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            const uint64_t md = m.md_msgs_total.load(std::memory_order_relaxed);
            const double tps = double(md - last_md);
            last_md = md;
            tps_->Set(tps);

            publish("md_processing", m.md_processing.snapshot());
            publish("tick_to_trade", m.tick_to_trade.snapshot());
            publish("queue_wait",    m.queue_wait.snapshot());

            set_counter(*c_md_,     double(md));
            set_counter(*c_drop_,   double(m.md_dropped_total.load(std::memory_order_relaxed)));
            set_counter(*c_orders_, double(m.orders_sent_total.load(std::memory_order_relaxed)));
            set_counter(*c_cxl_,    double(m.cancels_sent_total.load(std::memory_order_relaxed)));
            set_counter(*c_exec_,   double(m.execs_total.load(std::memory_order_relaxed)));

            g_mdq_->Set(double(m.md_queue_max.load(std::memory_order_relaxed)));
            g_ordq_->Set(double(m.ord_queue_max.load(std::memory_order_relaxed)));
            g_logq_->Set(double(m.log_queue_max.load(std::memory_order_relaxed)));

            const auto f1 = m.md_processing.snapshot();
            const auto f2 = m.tick_to_trade.snapshot();
            std::printf("[metrics] tps=%.0f | md_proc p50=%lld p90=%lld p99=%lld ns | t2t p50=%lld p90=%lld p99=%lld ns | mdq_max=%llu\n",
                tps,
                (long long)f1.p50, (long long)f1.p90, (long long)f1.p99,
                (long long)f2.p50, (long long)f2.p90, (long long)f2.p99,
                (unsigned long long)m.md_queue_max.load(std::memory_order_relaxed));
        }
    }

private:
    void publish(const char* flow, const LatencyHistogram::Percentiles& p) {
        gauges_[std::string(flow) + ":p50"]->Set(double(p.p50));
        gauges_[std::string(flow) + ":p90"]->Set(double(p.p90));
        gauges_[std::string(flow) + ":p99"]->Set(double(p.p99));
        gauges_[std::string(flow) + ":max"]->Set(double(p.max));
    }
    static void set_counter(prometheus::Counter& c, double target) {
        const double cur = c.Value();
        if (target > cur) c.Increment(target - cur);
    }

    prometheus::Exposer exposer_;
    std::shared_ptr<prometheus::Registry> registry_;
    std::map<std::string, prometheus::Gauge*> gauges_;
    prometheus::Gauge *tps_, *g_mdq_, *g_ordq_, *g_logq_;
    prometheus::Counter *c_md_, *c_drop_, *c_orders_, *c_cxl_, *c_exec_;
};
