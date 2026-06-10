#pragma once
// ----------------------------------------------------------------------------
// MarketMaker — the strategy hot loop. Runs on ONE pinned core.
//
// Per MD event:
//   1. pop MdEvent from the SPSC ring (busy-spin with pause)
//   2. apply full-refresh snapshot to the per-symbol OrderBook  } F1 stops
//   3. decision per the assignment:
//        spread >= MINIMUM_SPREAD  -> quote BestBid+tick / BestAsk-tick
//        spread <  MINIMUM_SPREAD  -> cancel all active quotes, stand down
//   4. emit OrderCmd PODs into the order ring (sender thread does FIX I/O)
//
// Everything in steps 1-4 is allocation-free and lock-free. The only writes
// shared with other cores are the two ring publishes and relaxed metric adds.
// ----------------------------------------------------------------------------
#include "../common/spsc_ring.hpp"
#include "../common/events.hpp"
#include "../common/timestamp.hpp"
#include "../common/config.hpp"
#include "../common/async_logger.hpp"
#include "../book/order_book.hpp"
#include "../oms/order_manager.hpp"
#include "../metrics/metrics.hpp"
#include "../storage/lmdb_persister.hpp"

#include <atomic>
#include <cmath>

#if defined(_MSC_VER)
  #include <immintrin.h>   // _mm_pause
#endif

using MdRing   = SpscRing<MdEvent,   1 << 14>;
using ExecRing = SpscRing<ExecEvent, 1 << 12>;
using OrdRing  = SpscRing<OrderCmd,  1 << 12>;

class MarketMaker {
public:
    MarketMaker(const EngineConfig& cfg, MdRing& md, ExecRing& ex, OrdRing& out,
                AsyncLogger& log, LmdbPersister& db)
        : cfg_(cfg), md_(md), exec_(ex), out_(out), log_(log), db_(db) {}

    void run(std::atomic<bool>& running) {
        auto& m = Metrics::instance();
        MdEvent ev; ExecEvent ex;
        int64_t next_snapshot_ns = ts::now_ns();
        const int64_t snap_every = int64_t(cfg_.snapshot_interval_ms) * 1'000'000;

        while (running.load(std::memory_order_relaxed)) {
            bool worked = false;

            // ---- drain exec reports first (order state must be fresh) ----
            while (exec_.try_pop(ex)) {
                oms_.on_exec(ex);
                if (ex.kind == ExecKind::Filled || ex.kind == ExecKind::PartFill) {
                    db_.push_trade(ex);                       // holdings history
                    LogRecord r{}; r.t_ns = ex.t_recv_ns; r.kind = LogKind::Exec;
                    r.symbol_id = ex.symbol_id; r.side = ex.side;
                    r.px = ex.last_px; r.qty = ex.last_qty; r.cl_ord_id = ex.cl_ord_id;
                    log_.log(r);
                }
                worked = true;
            }

            // ---- market data ----
            if (md_.try_pop(ev)) {
                worked = true;
                const int64_t t_deq = ts::now_ns();
                m.queue_wait.record(t_deq - ev.t_recv_ns);    // ring residency

                OrderBook& ob = books_[ev.symbol_id];
                ob.apply_snapshot(ev, cfg_.book_depth);
                m.md_processing.record(ts::now_ns() - ev.t_recv_ns);   // F1

                if (ob.has_bba()) decide(ev.symbol_id, ob, ev.t_recv_ns);

                // periodic OB snapshot to LMDB (cheap POD push, cold write)
                if (t_deq >= next_snapshot_ns) {
                    next_snapshot_ns = t_deq + snap_every;
                    push_snapshots(t_deq);
                }
            }

            if (!worked) cpu_relax();                         // polite busy-spin
        }
    }

private:
    // ------------------------- strategy decision ----------------------------
    void decide(uint32_t sym, const OrderBook& ob, int64_t t_md) {
        const double spread = ob.spread();

        if (spread >= cfg_.minimum_spread) {
            // WIDE market: improve BBA by one tick on both sides.
            const double bid = ob.best_bid() + cfg_.tick_size;
            const double ask = ob.best_ask() - cfg_.tick_size;
            if (ask - bid < cfg_.tick_size * 0.5) return;     // would cross: skip
            requote(sym, OrdSide::Buy,  bid, t_md);
            requote(sym, OrdSide::Sell, ask, t_md);
        } else {
            // NARROW market: risk-off, cancel everything for this symbol.
            cancel_side(sym, OrdSide::Buy, t_md);
            cancel_side(sym, OrdSide::Sell, t_md);
        }
    }

    void requote(uint32_t sym, OrdSide side, double px, int64_t t_md) {
        Quote& q = oms_.quote(sym, side);
        if (q.state == QuoteState::PendingNew || q.state == QuoteState::PendingCancel)
            return;                                            // in flight, wait
        if (q.state == QuoteState::Live) {
            if (std::fabs(q.price - px) < cfg_.tick_size * 0.5) return; // already there
            cancel_side(sym, side, t_md);                      // move: cancel first
            return;                                            // re-quote next tick
        }
        // state == None -> place a new order
        OrderCmd c{};
        c.type = OrdCmdType::New; c.side = side; c.symbol_id = sym;
        c.price = px; c.qty = cfg_.order_qty;
        c.cl_ord_id = next_cl_ord_id();
        c.t_md_ns = t_md; c.t_decided_ns = ts::now_ns();
        if (!cfg_.dry_run && out_.try_push(c)) {
            q.state = QuoteState::PendingNew; q.cl_ord_id = c.cl_ord_id; q.price = px; q.qty = c.qty;
            Metrics::instance().track_max(Metrics::instance().ord_queue_max, out_.size_approx());
            LogRecord r{}; r.t_ns = c.t_decided_ns; r.kind = LogKind::OrderNew;
            r.symbol_id = sym; r.side = side; r.px = px; r.qty = c.qty; r.cl_ord_id = c.cl_ord_id;
            log_.log(r);
        }
    }

    void cancel_side(uint32_t sym, OrdSide side, int64_t t_md) {
        Quote& q = oms_.quote(sym, side);
        if (q.state != QuoteState::Live) return;
        OrderCmd c{};
        c.type = OrdCmdType::Cancel; c.side = side; c.symbol_id = sym;
        c.qty = q.qty; c.orig_cl_ord_id = q.cl_ord_id;
        c.cl_ord_id = next_cl_ord_id();
        c.t_md_ns = t_md;
        c.t_decided_ns = ts::now_ns();
        if (!cfg_.dry_run && out_.try_push(c)) {
            q.state = QuoteState::PendingCancel;
            LogRecord r{}; r.t_ns = c.t_decided_ns; r.kind = LogKind::OrderCxl;
            r.symbol_id = sym; r.side = side; r.px = q.price; r.qty = q.qty;
            r.cl_ord_id = c.orig_cl_ord_id;
            log_.log(r);
        }
    }

    void push_snapshots(int64_t t) {
        for (uint32_t s = 0; s < kMaxSymbols; ++s) {
            const OrderBook& ob = books_[s];
            if (!ob.n_bids && !ob.n_asks) continue;
            BookSnapshot snap{};
            snap.t_ns = t; snap.symbol_id = s;
            snap.n_bids = ob.n_bids; snap.n_asks = ob.n_asks;
            std::memcpy(snap.bid_px,  ob.bid_px,  sizeof(snap.bid_px));
            std::memcpy(snap.bid_qty, ob.bid_qty, sizeof(snap.bid_qty));
            std::memcpy(snap.ask_px,  ob.ask_px,  sizeof(snap.ask_px));
            std::memcpy(snap.ask_qty, ob.ask_qty, sizeof(snap.ask_qty));
            db_.push_snapshot(snap);
        }
    }

    uint64_t next_cl_ord_id() noexcept { return cl_ord_seed_++; }

    static void cpu_relax() noexcept {
#if defined(_MSC_VER) || defined(__x86_64__)
        _mm_pause();
#else
        std::this_thread::yield();
#endif
    }

    const EngineConfig& cfg_;
    MdRing&   md_;
    ExecRing& exec_;
    OrdRing&  out_;
    AsyncLogger&   log_;
    LmdbPersister& db_;
    OrderManager oms_;
    OrderBook books_[kMaxSymbols];
    uint64_t cl_ord_seed_ = uint64_t(ts::now_ns() / 1'000'000) * 1000; // unique per day/run
};
