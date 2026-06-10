// mm-engine — low-latency market-making engine
//
// Thread layout (each pinned where it matters):
//   [QuickFIX reader]  --MdRing-->   [Strategy core]  --OrdRing-->  [Sender]
//          |                 \--ExecRing-^   |  \--SnapRing/TradeRing--> [LMDB]
//          |                                 \--LogRing--> [AsyncLogger]
//   [MetricsServer 1Hz]  -> Prometheus :9091 -> Grafana
//
// Modes:
//   mm_engine                          live trading vs reMarkets
//   mm_engine --bench [n_msgs]         offline load test: synthetic MD events
//                                      pushed through the full strategy path   
// -----------------------------------------------------------------------------
#include "common/config.hpp"
#include "common/spsc_ring.hpp"
#include "common/events.hpp"
#include "common/timestamp.hpp"
#include "common/affinity.hpp"
#include "common/async_logger.hpp"
#include "book/order_book.hpp"
#include "strategy/market_maker.hpp"
#include "fix/fix_app.hpp"
#include "metrics/metrics_server.hpp"
#include "storage/lmdb_persister.hpp"

#include <quickfix/SessionSettings.h>
#include <quickfix/FileStore.h>
#include <quickfix/FileLog.h>
#include <quickfix/SocketInitiator.h>

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <thread>

static std::atomic<bool> g_running{true};
static void on_signal(int) { g_running.store(false); }

static std::string read_password(const char* path) {
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("password=", 0) == 0) {
            line = line.substr(9);
            while (!line.empty() && (line.back()=='\r'||line.back()==' ')) line.pop_back();
            return line;
        }
    }
    return {};
}

// ---------------------------------------------------------------------------
// Offline load test: feeds synthetic full-refresh snapshots into MdRing at max
// rate. Exercises ring -> book -> strategy -> order ring exactly like prod
// (DRY_RUN forced), so a profiler see the real hot path.
// ---------------------------------------------------------------------------
static void run_bench(EngineConfig cfg, uint64_t n_msgs) {
    cfg.dry_run = false;   // we still want OrderCmds generated; sender just drains
    std::printf("[bench] generating %llu synthetic MD snapshots...\n",
                (unsigned long long)n_msgs);

    // HEAP-allocate the rings/stages: together they are several MB. Windows gives the main thread 1 mb, so we don't want a stack overflow error.
    auto md_p     = std::make_unique<MdRing>();
    auto ex_p     = std::make_unique<ExecRing>();
    auto out_p    = std::make_unique<OrdRing>();
    auto logger_p = std::make_unique<AsyncLogger>("ops_bench.csv");
    auto db_p     = std::make_unique<LmdbPersister>();
    MdRing& md = *md_p; ExecRing& ex = *ex_p; OrdRing& out = *out_p;
    AsyncLogger& logger = *logger_p; LmdbPersister& db = *db_p;
    if (!db.open(cfg.lmdb_path + "_bench"))
        std::fprintf(stderr, "[bench] warning: LMDB open failed, persisting disabled\n");

    auto mm_p = std::make_unique<MarketMaker>(cfg, md, ex, out, logger, db);
    MarketMaker& mm = *mm_p;

    std::thread t_log([&]{ logger.run(g_running); });
    std::thread t_db([&]{ db.run(g_running); });
    std::thread t_strat([&]{
        affinity::pin_this_thread(cfg.strategy_core);
        affinity::boost_priority();
        mm.run(g_running);
    });
    std::thread t_sink([&]{                     // drains OrdRing like the sender
        affinity::pin_this_thread(cfg.fix_core);
        OrderCmd c;
        auto& m = Metrics::instance();
        while (g_running.load(std::memory_order_relaxed)) {
            while (out.try_pop(c)) {
                if (c.type == OrdCmdType::New) {
                    m.tick_to_trade.record(ts::now_ns() - c.t_md_ns);
                    m.orders_sent_total.fetch_add(1, std::memory_order_relaxed);
                } else {
                    m.cancels_sent_total.fetch_add(1, std::memory_order_relaxed);
                }
            }
            std::this_thread::yield();
        }
    });

    // producer (this thread) pinned to its own core
    affinity::pin_this_thread(cfg.strategy_core + 2);
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> jig(-0.5, 0.5);
    const int n_syms = int(std::min<std::size_t>(cfg.symbols.size(), kMaxSymbols));
    const int64_t t0 = ts::now_ns();

    for (uint64_t i = 0; i < n_msgs && g_running.load(std::memory_order_relaxed); ++i) {
        MdEvent ev{};
        ev.t_recv_ns = ts::now_ns();
        ev.symbol_id = uint32_t(i % n_syms);
        ev.seq = uint32_t(i);
        const double mid = 1050.0 + jig(rng);
        const double half = (i % 7 == 0) ? 0.01 : 0.10;   // mix narrow/wide spreads
        ev.n_bids = ev.n_asks = 5;
        for (int d = 0; d < 5; ++d) {
            ev.bid_px[d] = mid - half - d * 0.05; ev.bid_qty[d] = 10 + d;
            ev.ask_px[d] = mid + half + d * 0.05; ev.ask_qty[d] = 10 + d;
        }
        Metrics::instance().md_msgs_total.fetch_add(1, std::memory_order_relaxed);
        while (!md.try_push(ev)) {                         // backpressure: spin
            if (!g_running.load(std::memory_order_relaxed)) break;
        }
        Metrics::instance().track_max(Metrics::instance().md_queue_max, md.size_approx());
    }
    const int64_t t1 = ts::now_ns();

    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // drain
    g_running.store(false);
    t_strat.join(); t_sink.join(); t_db.join(); t_log.join();

    auto& m = Metrics::instance();
    const auto f1 = m.md_processing.snapshot();
    const auto f2 = m.tick_to_trade.snapshot();
    const auto qw = m.queue_wait.snapshot();
    const double secs = double(t1 - t0) / 1e9;
    std::printf("\n========== BENCH RESULTS ==========\n");
    std::printf("messages        : %llu in %.3f s  ->  %.0f msg/s\n",
                (unsigned long long)n_msgs, secs, double(n_msgs) / secs);
    std::printf("F1 md_processing: p50=%lld  p90=%lld  p99=%lld  max=%lld ns\n",
                (long long)f1.p50, (long long)f1.p90, (long long)f1.p99, (long long)f1.max);
    std::printf("F2 tick_to_trade: p50=%lld  p90=%lld  p99=%lld  max=%lld ns\n",
                (long long)f2.p50, (long long)f2.p90, (long long)f2.p99, (long long)f2.max);
    std::printf("queue_wait      : p50=%lld  p90=%lld  p99=%lld ns\n",
                (long long)qw.p50, (long long)qw.p90, (long long)qw.p99);
    std::printf("backlog max     : md=%llu ord=%llu log=%llu\n",
                (unsigned long long)m.md_queue_max.load(),
                (unsigned long long)m.ord_queue_max.load(),
                (unsigned long long)m.log_queue_max.load());
    std::printf("orders generated: %llu (sink-drained)\n",
                (unsigned long long)m.orders_sent_total.load());
}

// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    EngineConfig cfg = EngineConfig::load("config/strategy.cfg");
    ts::cal().calibrate();

    if (argc > 1 && std::strcmp(argv[1], "--bench") == 0) {
        const uint64_t n = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 2'000'000;
        std::thread t_metrics;
        MetricsServer metrics(cfg.metrics_port);
        t_metrics = std::thread([&]{ metrics.run(g_running); });
        run_bench(cfg, n);
        t_metrics.join();
        return 0;
    }

    // ----------------------------- live mode --------------------------------
    try {
        FIX::SessionSettings settings(cfg.fix_cfg);
        // SenderCompID doubles as Username on ROFEX
        cfg.sender = settings.getSessions().begin()->getSenderCompID().getValue();

        // Heap-allocate the big stages (see run_bench comment: Windows 1 MB stack).
        auto md_p     = std::make_unique<MdRing>();
        auto ex_p     = std::make_unique<ExecRing>();
        auto out_p    = std::make_unique<OrdRing>();
        auto logger_p = std::make_unique<AsyncLogger>("ops_live.csv");
        auto db_p     = std::make_unique<LmdbPersister>();
        MdRing& md = *md_p; ExecRing& ex = *ex_p; OrdRing& out = *out_p;
        AsyncLogger& logger = *logger_p; LmdbPersister& db = *db_p;
        if (!db.open(cfg.lmdb_path)) { std::fprintf(stderr, "LMDB open failed\n"); return 1; }

        auto app_p = std::make_unique<FixApp>(cfg, md, ex);
        FixApp& app = *app_p;
        app.set_password(read_password("config/credentials.txt"));

        FIX::FileStoreFactory store(settings);
        FIX::FileLogFactory   log(settings);
        FIX::SocketInitiator  initiator(app, store, settings, log);

        auto mm_p = std::make_unique<MarketMaker>(cfg, md, ex, out, logger, db);
        MarketMaker& mm = *mm_p;
        MetricsServer metrics(cfg.metrics_port);

        std::thread t_metrics([&]{ metrics.run(g_running); });
        std::thread t_log    ([&]{ logger.run(g_running); });
        std::thread t_db     ([&]{ db.run(g_running); });
        std::thread t_strat  ([&]{
            affinity::pin_this_thread(cfg.strategy_core);
            affinity::boost_priority();
            mm.run(g_running);
        });
        std::thread t_sender ([&]{
            affinity::pin_this_thread(cfg.fix_core);
            OrderCmd c;
            while (g_running.load(std::memory_order_relaxed)) {
                bool worked = false;
                while (out.try_pop(c)) { if (app.logged_on()) app.send_order_cmd(c); worked = true; }
                if (!worked) std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        });

        initiator.start();
        std::printf("[main] engine up. Ctrl+C to stop.\n");
        while (g_running.load(std::memory_order_relaxed))
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

        std::printf("[main] shutting down...\n");
        initiator.stop();
        t_sender.join(); t_strat.join(); t_db.join(); t_log.join(); t_metrics.join();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "FATAL: %s\n", e.what());
        return 1;
    }
    return 0;
}
