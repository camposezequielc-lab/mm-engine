#pragma once
// ----------------------------------------------------------------------------
// AsyncLogger — operations log for post-trade strategy analysis.
//
// Hot path:  fill a fixed POD LogRecord, push to an SPSC ring (~20 ns).
// Cold path: a dedicated thread drains the ring and writes CSV with buffered
//            stdio. The hot path never touches the filesystem, never formats
//            strings, never allocates.
//
// Output: ops_YYYYMMDD.csv -> ts_ns,kind,symbol,side,px,qty,cl_ord_id,text
// ----------------------------------------------------------------------------
#include "../common/spsc_ring.hpp"
#include "../common/events.hpp"
#include "../metrics/metrics.hpp"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

class AsyncLogger {
public:
    explicit AsyncLogger(const std::string& path) {
        f_ = std::fopen(path.c_str(), "w");
        if (f_) std::fprintf(f_, "ts_ns,kind,symbol_id,side,px,qty,cl_ord_id,text\n");
    }
    ~AsyncLogger() { if (f_) std::fclose(f_); }

    // producer side (any pipeline thread. One ring per producer would be
    // strictly SPSC — for the exercise the strategy thread is the only
    // producer of business logs, FIX thread logs only on session events).
    bool log(const LogRecord& r) noexcept {
        const bool ok = ring_.try_push(r);
        Metrics::instance().track_max(Metrics::instance().log_queue_max, ring_.size_approx());
        return ok;
    }

    void log_text(LogKind k, const char* msg) noexcept {
        LogRecord r{};
        r.t_ns = 0; r.kind = k;
        std::strncpy(r.text, msg, sizeof(r.text) - 1);
        log(r);
    }

    //consumer thread
    void run(std::atomic<bool>& running) {
        static const char* kinds[] = {"MD","NEW","CXL","EXEC","INFO"};
        LogRecord r;
        while (running.load(std::memory_order_relaxed) || ring_.size_approx() > 0) {
            int drained = 0;
            while (ring_.try_pop(r)) {
                if (f_) std::fprintf(f_, "%lld,%s,%u,%u,%.6f,%.2f,%llu,%s\n",
                    (long long)r.t_ns, kinds[int(r.kind)], r.symbol_id,
                    unsigned(r.side), r.px, r.qty,
                    (unsigned long long)r.cl_ord_id, r.text);
                ++drained;
            }
            if (f_ && drained) std::fflush(f_);
            if (!drained) std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

private:
    SpscRing<LogRecord, 1 << 14> ring_;   // 16k records buffered
    std::FILE* f_ = nullptr;
};
