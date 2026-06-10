#!/usr/bin/env python3
"""
mock_exchange.py — a minimal local FIX 5.0SP2/FIXT.1.1 exchange simulator that
speaks enough of Primary/ROFEX's dialect to exercise the mm-engine end to end:

  * accepts Logon (any credentials), answers Logon, keeps heartbeats
  * answers MarketDataRequest (V) by streaming full-depth (5-level)
    MarketDataSnapshotFullRefresh (W) for every requested symbol, alternating
    every few seconds between a WIDE spread (strategy quotes both sides) and a
    LOCKED book (spread < MINIMUM_SPREAD -> strategy cancels everything)
  * answers NewOrderSingle (D) with ExecutionReport New (and fully fills every
    5th order so fills/trade-persistence are exercised too)
  * answers OrderCancelRequest (F) with ExecutionReport Canceled

Pure standard library. Usage:
    python scripts/mock_exchange.py [port]      # default 7000

Then point the engine at it: in config/strategy.cfg set
    FIX_CFG=config/fix_config_local.cfg
and run mm_engine.exe normally (no stunnel needed).
"""
import socket
import sys
import threading
import time
from datetime import datetime, timezone

SOH = "\x01"
HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7000

WIDE_SECONDS = 6.0          # seconds per phase
SNAPSHOT_INTERVAL = 0.25    # seconds between W per symbol
TICK = 0.5
BASE_PX = {0: 1470.0}       # per-symbol base prices get offset below


def now_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def checksum(s: str) -> str:
    return f"{sum(s.encode('latin-1')) % 256:03d}"


def build(msg_type, seq, sender, target, body_fields):
    """body_fields: list of (tag, value) AFTER the standard header fields."""
    body = f"35={msg_type}{SOH}34={seq}{SOH}49={sender}{SOH}52={now_ts()}{SOH}56={target}{SOH}"
    for t, v in body_fields:
        body += f"{t}={v}{SOH}"
    head = f"8=FIXT.1.1{SOH}9={len(body)}{SOH}"
    full = head + body
    return (full + f"10={checksum(full)}{SOH}").encode("latin-1")


def parse(raw: str):
    d, groups55 = {}, []
    for kv in raw.split(SOH):
        if "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        if k == "55":
            groups55.append(v)
        d.setdefault(k, v)          # first occurrence wins for scalars
    d["_symbols"] = groups55
    return d


class Session(threading.Thread):
    def __init__(self, conn, addr):
        super().__init__(daemon=True)
        self.conn, self.addr = conn, addr
        self.seq = 0
        self.user = "CLIENT"
        self.lock = threading.Lock()
        self.alive = True
        self.md_thread = None
        self.order_no = 0

    # ---- low-level ----
    def send_msg(self, msg_type, fields):
        with self.lock:
            self.seq += 1
            data = build(msg_type, self.seq, "ROFX", self.user, fields)
            try:
                self.conn.sendall(data)
            except OSError:
                self.alive = False

    # ---- market data ----
    def md_stream(self, req_id, symbols):
        print(f"[mock] streaming MD for {symbols} (req {req_id})")
        t0 = time.time()
        while self.alive:
            wide = int((time.time() - t0) / WIDE_SECONDS) % 2 == 0
            for i, sym in enumerate(symbols):
                base = 1470.0 + 10.0 * i
                if wide:
                    bb, ba = base - 2 * TICK, base + 2 * TICK   # spread 2.0 >= 0.05
                else:
                    bb = ba = base                              # locked: spread 0 < 0.05
                entries = []
                for lvl in range(5):
                    entries.append(("269", "0"))
                    entries.append(("270", f"{bb - lvl * TICK:.1f}"))
                    entries.append(("271", str(10 * (lvl + 1))))
                for lvl in range(5):
                    entries.append(("269", "1"))
                    entries.append(("270", f"{ba + lvl * TICK:.1f}"))
                    entries.append(("271", str(10 * (lvl + 1))))
                fields = [("262", req_id), ("55", sym), ("207", "ROFX"),
                          ("268", "10")] + entries
                self.send_msg("W", fields)
            time.sleep(SNAPSHOT_INTERVAL)

    # ---- order handling ----
    def exec_report(self, m, exec_type, ord_status, leaves, cum, last_px=None, last_qty=None):
        self.order_no += 1
        qty = m.get("38", "1")
        f = [("37", f"MOCK{self.order_no:06d}"),
             ("17", f"E{self.order_no:06d}"),
             ("150", exec_type), ("39", ord_status),
             ("11", m.get("11", "")),
             ("55", m.get("55", m["_symbols"][0] if m["_symbols"] else "")),
             ("207", "ROFX"),
             ("54", m.get("54", "1")),
             ("38", qty),
             ("151", leaves), ("14", cum),
             ("60", now_ts())]
        if m.get("44"):
            f.append(("44", m["44"]))
        if last_px is not None:
            f += [("31", last_px), ("32", last_qty)]
        self.send_msg("8", f)

    # ---- main loop ----
    def run(self):
        print(f"[mock] connection from {self.addr}")
        buf = ""
        try:
            while self.alive:
                chunk = self.conn.recv(65536)
                if not chunk:
                    break
                buf += chunk.decode("latin-1")
                while True:
                    end = buf.find(SOH + "10=")
                    if end < 0:
                        break
                    end = buf.find(SOH, end + 1)
                    if end < 0:
                        break
                    raw, buf = buf[:end + 1], buf[end + 1:]
                    self.handle(parse(raw))
        finally:
            self.alive = False
            self.conn.close()
            print(f"[mock] connection {self.addr} closed")

    def handle(self, m):
        mt = m.get("35")
        if mt == "A":
            self.user = m.get("49", "CLIENT")
            print(f"[mock] LOGON from {self.user} (pwd accepted, any)")
            fields = [("98", "0"), ("108", m.get("108", "30")), ("1137", "9")]
            if m.get("141") == "Y":
                fields.insert(2, ("141", "Y"))
            self.send_msg("A", fields)
        elif mt == "0":
            pass                                            # client heartbeat
        elif mt == "1":
            self.send_msg("0", [("112", m.get("112", ""))])  # test request
        elif mt == "5":
            print("[mock] LOGOUT")
            self.send_msg("5", [])
            self.alive = False
        elif mt == "V":
            syms = m["_symbols"] or ["DLR/JUN26"]
            if self.md_thread is None:
                self.md_thread = threading.Thread(
                    target=self.md_stream, args=(m.get("262", "1"), syms), daemon=True)
                self.md_thread.start()
        elif mt == "D":
            print(f"[mock] NewOrderSingle {m.get('11')} {m.get('55')} side={m.get('54')} px={m.get('44')}")
            qty = m.get("38", "1")
            self.exec_report(m, "0", "0", leaves=qty, cum="0")          # New
            if self.order_no % 5 == 0:                                   # every 5th: fill
                self.exec_report(m, "F", "2", leaves="0", cum=qty,
                                 last_px=m.get("44", "0"), last_qty=qty)
                print(f"[mock]   -> filled {m.get('11')}")
        elif mt == "F":
            print(f"[mock] CancelRequest {m.get('11')} (orig {m.get('41')})")
            self.exec_report(m, "4", "4", leaves="0", cum="0")           # Canceled
        else:
            print(f"[mock] ignoring 35={mt}")


def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, PORT))
    srv.listen(4)
    print(f"[mock] ROFX mock exchange listening on {HOST}:{PORT} — Ctrl+C to stop")
    print(f"[mock] phases: {WIDE_SECONDS:.0f}s WIDE spread (engine quotes) / "
          f"{WIDE_SECONDS:.0f}s LOCKED book (engine cancels)")
    try:
        while True:
            conn, addr = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            Session(conn, addr).start()
    except KeyboardInterrupt:
        print("\n[mock] bye")


if __name__ == "__main__":
    main()
