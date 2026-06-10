#!/usr/bin/env python3
"""
Dump the engine's LMDB store: order-book snapshots and trade history.

Usage:
    pip install lmdb
    python scripts/dump_lmdb.py [path-to-lmdb-file] [--ob] [--tr] [--limit N]

Default path: data/lmdb (the engine opens it with MDB_NOSUBDIR, so the path is
a single file; a "-lock" sibling file appears next to it).

Key layout written by src/persist/lmdb_persister.hpp:
    ob|<symbol_id>|<seq>  -> BookSnapshot POD (packed below)
    tr|<seq>              -> ExecEvent POD (fills only = trade/holdings history)
"""
import argparse
import datetime
import struct
import sys

try:
    import lmdb
except ImportError:
    sys.exit("Missing dependency. Run:  pip install lmdb")

KMAXDEPTH = 5

# Mirrors src/common/events.hpp (x86-64, little-endian, natural alignment):
# struct BookSnapshot { int64 t_ns; uint32 symbol_id; uint16 n_bids, n_asks;
#                       double bid_px[5], bid_qty[5], ask_px[5], ask_qty[5]; };
SNAP_FMT = "<qIHH" + "d" * (4 * KMAXDEPTH)          # 176 bytes
# struct ExecEvent { int64 t_recv_ns; uint64 cl_ord_id; uint8 kind; uint8 side;
#                    uint16 _pad; uint32 symbol_id; double last_px, last_qty,
#                    leaves_qty; char order_id[24]; };
EXEC_FMT = "<qQBBHIddd24s"                          # 72 bytes

EXEC_KIND = {0: "New", 1: "Canceled", 2: "Filled", 3: "PartFill",
             4: "Rejected", 5: "Replaced", 6: "Other"}
SIDE = {0: "Buy", 1: "Sell"}


def fmt_ns(ts_ns):
    return datetime.datetime.fromtimestamp(ts_ns / 1e9).strftime("%Y-%m-%d %H:%M:%S.%f")


def dump(path, show_ob, show_tr, limit):
    env = lmdb.open(path, subdir=False, readonly=True, lock=False,
                    max_dbs=0)
    n_ob = n_tr = tot_ob = tot_tr = 0
    with env.begin() as txn:
        for k, v in txn.cursor():
            key = k.decode("ascii", "replace")
            if key.startswith("ob|"):
                tot_ob += 1
                if show_ob and n_ob < limit and len(v) >= struct.calcsize(SNAP_FMT):
                    n_ob += 1
                    f = struct.unpack_from(SNAP_FMT, v)
                    t_ns, sym, nb, na = f[0], f[1], f[2], f[3]
                    d = f[4:]
                    bb = d[0] if nb else float("nan")
                    ba = d[2 * KMAXDEPTH] if na else float("nan")
                    print(f"[OB ] {key:<24} {fmt_ns(t_ns)} sym={sym} "
                          f"bids={nb} asks={na} BB={bb:.4f} BA={ba:.4f}")
            elif key.startswith("tr|"):
                tot_tr += 1
                if show_tr and n_tr < limit and len(v) >= struct.calcsize(EXEC_FMT):
                    n_tr += 1
                    (t_ns, cl, kind, side, _pad, sym,
                     px, qty, leaves, oid) = struct.unpack_from(EXEC_FMT, v)
                    oid = oid.split(b"\0")[0].decode("ascii", "replace")
                    print(f"[TRD] {key:<24} {fmt_ns(t_ns)} sym={sym} "
                          f"{EXEC_KIND.get(kind, kind):<8} {SIDE.get(side, side):<4} "
                          f"px={px:.4f} qty={qty:g} leaves={leaves:g} "
                          f"clordid={cl} orderid={oid}")
    print(f"\nTotals in store: {tot_ob} order-book snapshots, {tot_tr} trade records.")
    env.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", nargs="?", default="data/lmdb")
    ap.add_argument("--ob", action="store_true", help="show only snapshots")
    ap.add_argument("--tr", action="store_true", help="show only trades")
    ap.add_argument("--limit", type=int, default=20, help="max rows per section")
    a = ap.parse_args()
    both = not (a.ob or a.tr)
    dump(a.path, a.ob or both, a.tr or both, a.limit)
