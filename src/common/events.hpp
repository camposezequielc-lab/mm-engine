#pragma once
// ----------------------------------------------------------------------------
// POD event types flowing through the SPSC rings. Everything is fixed-size and
// trivially copyable: no strings, no heap, no destructors on the hot path.
// Symbols are interned to a small integer id at startup.
// ----------------------------------------------------------------------------
#include <cstdint>

constexpr int kMaxSymbols  = 8;    // exercise uses 6 DLR futures
constexpr int kMaxDepth    = 5;    // ROFEX spec: maximum book depth = 5
constexpr int kSymbolLen   = 16;

// --- Market data event: one full-refresh snapshot for one instrument --------
struct MdEvent {
    int64_t  t_recv_ns;            // timestamp taken at fromApp() entry
    uint32_t symbol_id;
    uint16_t n_bids;
    uint16_t n_asks;
    double   bid_px[kMaxDepth];
    double   bid_qty[kMaxDepth];
    double   ask_px[kMaxDepth];
    double   ask_qty[kMaxDepth];
    double   last_px;              // 0 if absent
    uint32_t seq;                  // engine-local sequence
    uint32_t _pad;
};
static_assert(sizeof(MdEvent) <= 256, "keep MdEvent within 4 cache lines");

// --- Order command: strategy -> sender thread --------------------------------
enum class OrdCmdType : uint8_t { New = 0, Cancel = 1 };
enum class OrdSide    : uint8_t { Buy = 1, Sell = 2 };

struct OrderCmd {
    OrdCmdType type;
    OrdSide    side;
    uint16_t   _pad0;
    uint32_t   symbol_id;
    double     price;
    double     qty;
    uint64_t   cl_ord_id;          // numeric; serialized as string at the edge
    uint64_t   orig_cl_ord_id;     // for cancels
    int64_t    t_md_ns;            // originating MD timestamp (tick-to-trade)
    int64_t    t_decided_ns;       // when strategy decided
};

// --- Execution / order-state event: FIX thread -> strategy -------------------
enum class ExecKind : uint8_t { New=0, Canceled=1, Filled=2, PartFill=3, Rejected=4, Replaced=5, Other=6 };

struct ExecEvent {
    int64_t  t_recv_ns;
    uint64_t cl_ord_id;
    ExecKind kind;
    OrdSide  side;
    uint16_t _pad;
    uint32_t symbol_id;
    double   last_px;
    double   last_qty;
    double   leaves_qty;
    char     order_id[24];         // exchange OrderID (needed for cancel-by-id)
};

// --- Async log record ---------------------------------------------------------
enum class LogKind : uint8_t { MdTick=0, OrderNew=1, OrderCxl=2, Exec=3, Info=4 };

struct LogRecord {
    int64_t  t_ns;
    LogKind  kind;
    OrdSide  side;
    uint16_t _pad;
    uint32_t symbol_id;
    double   px;
    double   qty;
    uint64_t cl_ord_id;
    char     text[48];             // short free-form note (truncated, no alloc)
};

// --- Book snapshot for persistence 
struct BookSnapshot {
    int64_t  t_ns;
    uint32_t symbol_id;
    uint16_t n_bids, n_asks;
    double   bid_px[kMaxDepth], bid_qty[kMaxDepth];
    double   ask_px[kMaxDepth], ask_qty[kMaxDepth];
};
