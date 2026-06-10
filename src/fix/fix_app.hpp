#pragma once
// ----------------------------------------------------------------------------
// the QuickFIX boundary of the engine.
// Threading model:
//  * QuickFIX's reader thread calls fromApp()/fromAdmin(). We do the minimum
//    there: timestamp, extract numeric fields into a POD MdEvent/ExecEvent,
//    push to the SPSC ring. No strategy logic, no logging I/O, no allocation
//    beyond what QuickFIX itself already did parsing the message.
//  * An order-sender loop (run by main as a dedicated pinned thread) pops
//    OrderCmd from the strategy's output ring, builds the FIX message and
//    calls Session::sendToTarget(). The tick-to-trade clock stops right after
//    the send call returns (message handed to the socket layer).
//
// Spec mapping:
//  - Logon (A):   Username(553) + Password(554), EncryptMethod(98)=0,
//                 DefaultApplVerID(1137)=9 — injected in toAdmin().
//  - MD Request (V): SubscriptionRequestType=1 (snapshot+updates),
//                 MDUpdateType=0 (full refresh), AggregatedBook=Y,
//                 MarketDepth<=5, entries 0/1/2, Instrument= Symbol+ROFX.
//  - NewOrderSingle (D): Account(1), ClOrdID(11), OrderQty(38), OrdType(40)=2,
//                 Price(44), Side(54), TimeInForce(59)=0, TransactTime(60),
//                 Symbol(55), SecurityExchange(207)=ROFX.
//  - OrderCancelRequest (F): ClOrdID, OrigClOrdID, Side, TransactTime,
//                 Account, OrderQty, Symbol, SecurityExchange.
// ----------------------------------------------------------------------------
#include "../common/spsc_ring.hpp"
#include "../common/events.hpp"
#include "../common/timestamp.hpp"
#include "../common/config.hpp"
#include "../metrics/metrics.hpp"

#include <quickfix/Application.h>
#include <quickfix/Session.h>
#include <quickfix/MessageCracker.h>
#include <quickfix/fix50sp2/MarketDataSnapshotFullRefresh.h>
#include <quickfix/fix50sp2/MarketDataRequest.h>
#include <quickfix/fix50sp2/NewOrderSingle.h>
#include <quickfix/fix50sp2/OrderCancelRequest.h>
#include <quickfix/fix50sp2/ExecutionReport.h>
#include <quickfix/fixt11/Logon.h>

#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>

using MdRing   = SpscRing<MdEvent,   1 << 14>;   // 16384 snapshots
using ExecRing = SpscRing<ExecEvent, 1 << 12>;
using OrdRing  = SpscRing<OrderCmd,  1 << 12>;

class FixApp : public FIX::Application, public FIX::MessageCracker {
public:
    FixApp(const EngineConfig& cfg, MdRing& md_ring, ExecRing& exec_ring)
        : cfg_(cfg), md_ring_(md_ring), exec_ring_(exec_ring) {
        for (std::size_t i = 0; i < cfg.symbols.size() && i < kMaxSymbols; ++i)
            symbols_[i] = cfg.symbols[i];
        n_symbols_ = static_cast<uint32_t>(std::min<std::size_t>(cfg.symbols.size(), kMaxSymbols));
    }

    bool logged_on() const noexcept { return logged_on_.load(std::memory_order_acquire); }
    const FIX::SessionID& session() const { return session_id_; }

    // ---------------- FIX::Application ----------------
    void onCreate(const FIX::SessionID& sid) override { session_id_ = sid; }

    void onLogon(const FIX::SessionID& sid) override {
        std::printf("[fix] LOGON %s\n", sid.toString().c_str());
        logged_on_.store(true, std::memory_order_release);
        subscribe_market_data();
    }

    void onLogout(const FIX::SessionID& sid) override {
        std::printf("[fix] LOGOUT %s\n", sid.toString().c_str());
        logged_on_.store(false, std::memory_order_release);
    }

    void toAdmin(FIX::Message& msg, const FIX::SessionID&) override {
        FIX::MsgType mt;
        msg.getHeader().getField(mt);
        if (mt == FIX::MsgType_Logon) {                 // 553/554 per spec
            msg.setField(FIX::Username(cfg_.sender));
            msg.setField(FIX::Password(password_));
        }
    }

    void fromAdmin(const FIX::Message&, const FIX::SessionID&) noexcept override {}

    void toApp(FIX::Message&, const FIX::SessionID&) noexcept override {}

    // HOT PATH ENTRY — keep this lean.
    void fromApp(const FIX::Message& msg, const FIX::SessionID& sid) noexcept override {
        try {
            crack(msg, sid);   // dispatches to onMessage(); throws
                               // UnsupportedMessageType for types we don't crack
        } catch (const std::exception& e) {
            std::printf("[fix] fromApp dropped message: %s\n", e.what());
        } catch (...) {}
    }

    // Market Data Snapshot 
    void onMessage(const FIX50SP2::MarketDataSnapshotFullRefresh& msg,
                   const FIX::SessionID&) override {
        const int64_t t0 = ts::now_ns();                 // F1/F2 clock starts

        MdEvent ev{};
        ev.t_recv_ns = t0;
        ev.seq = ++md_seq_;

        FIX::Symbol sym;
        msg.get(sym);
        ev.symbol_id = symbol_id(sym.getValue());
        if (ev.symbol_id == UINT32_MAX) return;          // not ours

        FIX::NoMDEntries n;
        msg.get(n);
        const int count = n.getValue();
        FIX50SP2::MarketDataSnapshotFullRefresh::NoMDEntries g;
        FIX::MDEntryType  type;
        FIX::MDEntryPx    px;
        FIX::MDEntrySize  sz;

        for (int i = 1; i <= count; ++i) {
            msg.getGroup(i, g);
            g.get(type);
            const char t = type.getValue();
            double p = 0, q = 0;
            if (g.isSet(px)) { g.get(px); p = px.getValue(); }
            if (g.isSet(sz)) { g.get(sz); q = sz.getValue(); }
            if (t == FIX::MDEntryType_BID && ev.n_bids < kMaxDepth) {
                ev.bid_px[ev.n_bids] = p; ev.bid_qty[ev.n_bids] = q; ++ev.n_bids;
            } else if (t == FIX::MDEntryType_OFFER && ev.n_asks < kMaxDepth) {
                ev.ask_px[ev.n_asks] = p; ev.ask_qty[ev.n_asks] = q; ++ev.n_asks;
            } else if (t == FIX::MDEntryType_TRADE) {
                ev.last_px = p;
            }
        }

        auto& m = Metrics::instance();
        m.md_msgs_total.fetch_add(1, std::memory_order_relaxed);
        if (!md_ring_.try_push(ev))
            m.md_dropped_total.fetch_add(1, std::memory_order_relaxed);
        m.track_max(m.md_queue_max, md_ring_.size_approx());
    }

    // ---- Execution Report (8) ----
    void onMessage(const FIX50SP2::ExecutionReport& msg, const FIX::SessionID&) override {
        ExecEvent ev{};
        ev.t_recv_ns = ts::now_ns();

        FIX::ClOrdID cl;
        if (msg.isSet(cl)) { msg.get(cl); ev.cl_ord_id = std::strtoull(cl.getValue().c_str(), nullptr, 10); }

        FIX::ExecType et;
        if (msg.isSet(et)) {
            msg.get(et);
            switch (et.getValue()) {
                case FIX::ExecType_NEW:            ev.kind = ExecKind::New;      break;
                case FIX::ExecType_CANCELED:       ev.kind = ExecKind::Canceled; break;
                case FIX::ExecType_TRADE:          ev.kind = ExecKind::Filled;   break;
                case FIX::ExecType_REJECTED:       ev.kind = ExecKind::Rejected; break;
                case FIX::ExecType_REPLACED:       ev.kind = ExecKind::Replaced; break;
                default:                           ev.kind = ExecKind::Other;    break;
            }
        }
        FIX::OrdStatus os;
        if (ev.kind == ExecKind::Filled && msg.isSet(os)) {
            msg.get(os);
            if (os.getValue() == FIX::OrdStatus_PARTIALLY_FILLED) ev.kind = ExecKind::PartFill;
        }

        FIX::Side side;
        if (msg.isSet(side)) { msg.get(side); ev.side = side.getValue() == FIX::Side_BUY ? OrdSide::Buy : OrdSide::Sell; }
        FIX::Symbol sym;
        if (msg.isSet(sym)) { msg.get(sym); ev.symbol_id = symbol_id(sym.getValue()); }
        FIX::LastPx lpx;     if (msg.isSet(lpx)) { msg.get(lpx); ev.last_px = lpx.getValue(); }
        FIX::LastQty lqty;   if (msg.isSet(lqty)) { msg.get(lqty); ev.last_qty = lqty.getValue(); }
        FIX::LeavesQty lv;   if (msg.isSet(lv))  { msg.get(lv);  ev.leaves_qty = lv.getValue(); }
        FIX::OrderID oid;
        if (msg.isSet(oid)) {
            msg.get(oid);
            std::strncpy(ev.order_id, oid.getValue().c_str(), sizeof(ev.order_id) - 1);
        }

        Metrics::instance().execs_total.fetch_add(1, std::memory_order_relaxed);
        exec_ring_.try_push(ev);
    }

    void set_password(std::string pw) { password_ = std::move(pw); }

    // ---------------- outbound: called by the sender thread ----------------
    void send_order_cmd(const OrderCmd& c) {
        char clbuf[24];
        std::snprintf(clbuf, sizeof(clbuf), "%llu", (unsigned long long)c.cl_ord_id);

        if (c.type == OrdCmdType::New) {
            FIX50SP2::NewOrderSingle o(
                FIX::ClOrdID(clbuf),
                FIX::Side(c.side == OrdSide::Buy ? FIX::Side_BUY : FIX::Side_SELL),
                FIX::TransactTime(),
                FIX::OrdType(FIX::OrdType_LIMIT));
            o.set(FIX::Account(cfg_.account));
            o.set(FIX::Symbol(symbols_[c.symbol_id]));
            o.set(FIX::SecurityExchange("ROFX"));
            o.set(FIX::OrderQty(c.qty));
            o.set(FIX::Price(c.price));
            o.set(FIX::TimeInForce(FIX::TimeInForce_DAY));
            FIX::Session::sendToTarget(o, session_id_);
            // F2 clock stops here: message serialized + handed to socket layer
            Metrics::instance().tick_to_trade.record(ts::now_ns() - c.t_md_ns);
            Metrics::instance().orders_sent_total.fetch_add(1, std::memory_order_relaxed);
        } else {
            char origbuf[24];
            std::snprintf(origbuf, sizeof(origbuf), "%llu", (unsigned long long)c.orig_cl_ord_id);
            // FIX 5.0SP2: OrigClOrdID is optional on OrderCancelRequest, so the
            // generated ctor takes only (ClOrdID, Side, TransactTime).
            FIX50SP2::OrderCancelRequest x(
                FIX::ClOrdID(clbuf),
                FIX::Side(c.side == OrdSide::Buy ? FIX::Side_BUY : FIX::Side_SELL),
                FIX::TransactTime());
            x.set(FIX::OrigClOrdID(origbuf));
            x.set(FIX::Account(cfg_.account));
            x.set(FIX::Symbol(symbols_[c.symbol_id]));
            x.set(FIX::SecurityExchange("ROFX"));
            x.set(FIX::OrderQty(c.qty));
            FIX::Session::sendToTarget(x, session_id_);
            Metrics::instance().cancels_sent_total.fetch_add(1, std::memory_order_relaxed);
        }
    }

private:
    void subscribe_market_data() {
        FIX50SP2::MarketDataRequest req(
            FIX::MDReqID("MM-SUB-1"),
            FIX::SubscriptionRequestType(FIX::SubscriptionRequestType_SNAPSHOT_PLUS_UPDATES),
            FIX::MarketDepth(cfg_.book_depth));         // spec max 5
        req.set(FIX::MDUpdateType(FIX::MDUpdateType_FULL_REFRESH));
        req.set(FIX::AggregatedBook(true));

        for (const char t : {FIX::MDEntryType_BID, FIX::MDEntryType_OFFER, FIX::MDEntryType_TRADE}) {
            FIX50SP2::MarketDataRequest::NoMDEntryTypes g;
            g.set(FIX::MDEntryType(t));
            req.addGroup(g);
        }
        for (uint32_t i = 0; i < n_symbols_; ++i) {
            FIX50SP2::MarketDataRequest::NoRelatedSym g;
            g.set(FIX::Symbol(symbols_[i]));
            g.set(FIX::SecurityExchange("ROFX"));
            req.addGroup(g);
        }
        FIX::Session::sendToTarget(req, session_id_);
        std::printf("[fix] MarketDataRequest sent for %u symbols, depth=%d\n",
                    n_symbols_, cfg_.book_depth);
    }

    // 6 symbols -> linear scan over interned strings beats any hash here
    // (it fits in L1, branch predictor learns it. It takes ~5-15 ns).
    uint32_t symbol_id(const std::string& s) const noexcept {
        for (uint32_t i = 0; i < n_symbols_; ++i)
            if (symbols_[i] == s) return i;
        return UINT32_MAX;
    }

    const EngineConfig& cfg_;
    MdRing&   md_ring_;
    ExecRing& exec_ring_;
    std::array<std::string, kMaxSymbols> symbols_;
    uint32_t n_symbols_ = 0;
    std::string password_;
    FIX::SessionID session_id_;
    std::atomic<bool> logged_on_{false};
    uint32_t md_seq_ = 0;
};
