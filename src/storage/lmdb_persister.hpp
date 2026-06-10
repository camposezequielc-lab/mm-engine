#pragma once
// ----------------------------------------------------------------------------
// LmdbPersister — periodic Order Book snapshots + trade/holdings history.
//
// Hot-path impact minimized by design:
//  - The strategy thread only pushes fixed-size PODs into an SPSC ring.
//    Serialization (the structs ARE the serialized form — packed PODs) and
//    the mdb_txn/mdb_put/commit cycle happen on this cold thread.
//  - Writes are batched: one LMDB transaction per drain cycle, not per item.
//  -LMDB is memory-mapped + copy-on-write, so a put is mostly a memcpy into
//    the page cache; the commit fsync cost is paid here, never by the strategy.
// ----------------------------------------------------------------------------

#include "../common/spsc_ring.hpp"
#include "../common/events.hpp"
#include <lmdb.h>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <filesystem>

class LmdbPersister {
public:
    bool open(const std::string& path) {
        std::error_code ec;
        const std::filesystem::path p(path);
        if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path(), ec);
        if (mdb_env_create(&env_)) return false;
        mdb_env_set_mapsize(env_, std::size_t(1) << 30);   // 1 GiB map
        // MDB_NOSYNC trades durability for latency on the persister itself;
        // acceptable for analytics snapshots (state is reproducible).
        if (mdb_env_open(env_, path.c_str(), MDB_NOSUBDIR | MDB_NOSYNC, 0664)) return false;
        MDB_txn* txn;
        if (mdb_txn_begin(env_, nullptr, 0, &txn)) return false;
        if (mdb_dbi_open(txn, nullptr, 0, &dbi_)) { mdb_txn_abort(txn); return false; }
        mdb_txn_commit(txn);
        return true;
    }

    ~LmdbPersister() { if (env_) mdb_env_close(env_); }

    bool push_snapshot(const BookSnapshot& s) noexcept { return snap_ring_.try_push(s); }
    bool push_trade(const ExecEvent& e)      noexcept { return trade_ring_.try_push(e); }

    void run(std::atomic<bool>& running) {
        BookSnapshot s; ExecEvent e;
        while (running.load(std::memory_order_relaxed)
               || snap_ring_.size_approx() || trade_ring_.size_approx()) {
            int n = 0;
            MDB_txn* txn = nullptr;
            while (snap_ring_.try_pop(s)) {
                if (!txn && mdb_txn_begin(env_, nullptr, 0, &txn)) { txn = nullptr; break; }
                char key[40];
                const int kl = std::snprintf(key, sizeof(key), "ob|%u|%010llu",
                                             s.symbol_id, (unsigned long long)snap_seq_++);
                put(txn, key, kl, &s, sizeof(s));
                ++n;
            }
            while (trade_ring_.try_pop(e)) {
                if (!txn && mdb_txn_begin(env_, nullptr, 0, &txn)) { txn = nullptr; break; }
                char key[32];
                const int kl = std::snprintf(key, sizeof(key), "tr|%010llu",
                                             (unsigned long long)trade_seq_++);
                put(txn, key, kl, &e, sizeof(e));
                ++n;
            }
            if (txn) mdb_txn_commit(txn);                 // one commit per batch
            if (!n) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        mdb_env_sync(env_, 1);                            // final durable sync
    }

private:
    static void put(MDB_txn* txn, const char* key, int klen, const void* v, std::size_t vlen) {
        MDB_dbi dbi; mdb_dbi_open(txn, nullptr, 0, &dbi);
        MDB_val k{ std::size_t(klen), const_cast<char*>(key) };
        MDB_val d{ vlen, const_cast<void*>(v) };
        mdb_put(txn, dbi, &k, &d, 0);
    }

    MDB_env* env_ = nullptr;
    MDB_dbi  dbi_{};
    uint64_t snap_seq_ = 0, trade_seq_ = 0;
    SpscRing<BookSnapshot, 1 << 12> snap_ring_;
    SpscRing<ExecEvent,    1 << 12> trade_ring_;
};
