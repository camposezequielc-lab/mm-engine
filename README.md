# mm-engine — Low-Latency Market Making Engine (ROFEX / reMarkets)

A C++17 market-making engine for 6 DLR futures on Primary's reMarkets simulator,
connected over FIX 5.0 SP2 (QuickFIX), with lock-free internals, LMDB persistence,
and a Prometheus + Grafana observability stack.

```
                ┌─────────────────────────  HOT PATH  ─────────────────────────┐
 reMarkets ──TLS── stunnel ──TCP── QuickFIX reader ──[SPSC MdRing]── Strategy core
 (FIX 5.0SP2)                      (parse W/8 → POD)                 (book + decide)
                                        ▲                                │
                                        │                          [SPSC OrdRing]
                                   [ExecRing]                            │
                                        │                                ▼
                                        └──────────────────────── Sender thread ──► FIX D/F out
 COLD PATH:
   Strategy ──[SnapRing/TradeRing]──► LMDB thread (batched txns)
   Strategy ──[LogRing]────────────► Async CSV logger (ops_live.csv)
   Metrics thread (1 Hz) ──────────► Prometheus :9091 ──► Grafana :3000
```

## Design summary

**Threading.** One pipeline stage per thread, communicating exclusively through
single-producer/single-consumer lock-free ring buffers (`src/common/spsc_ring.hpp`).
There are no mutexes anywhere on the hot path; the only shared writes are ring
publishes (release stores) and relaxed atomic metric counters. The strategy thread
and the order sender are pinned to dedicated cores (`STRATEGY_CORE`, `FIX_CORE` in
`config/strategy.cfg`) and busy-spin with `_mm_pause`, which removes scheduler
wake-up latency (typically 5–50 µs per wake) from the tick-to-trade path.

**Memory.** Every event crossing a thread boundary is a fixed-size trivially-copyable
POD (`src/common/events.hpp`); rings are preallocated arrays, so the steady-state hot
path performs zero heap allocations. Head/tail indices live on separate cache lines
with locally cached copies of the opposite index, so the common push/pop touches only
one line and avoids cross-core coherence traffic. An `ObjectPool` is provided for
stages that need transient objects.

**Order book.** Market data from ROFEX arrives as Snapshot/Full Refresh (the engine
requests `MDUpdateType=0`), and the spec caps depth at 5. The optimal structure is
therefore two flat arrays per side inside a cache-line-aligned struct: applying a
snapshot is a straight copy, and best bid/ask is element zero — no trees, no hashing,
no pointer chasing. Depth is configurable 1–5 (`BOOK_DEPTH`). The theoretical price
uses the mid by default; a depth-weighted VWAP is implemented with AVX2 FMA intrinsics
(`src/book/order_book.hpp`) with a scalar fallback.

**Strategy (per the assignment).** On every snapshot the engine computes the market
spread. If `spread >= MINIMUM_SPREAD` it quotes a buy at `BestBid + 1 tick` and a sell
at `BestAsk - 1 tick`, aiming to become the new BBA and capture the spread; quotes are
moved by cancel-then-requote when the market moves more than a tick. If
`spread < MINIMUM_SPREAD` the market is considered tight and all working orders are
cancelled. One working quote per symbol/side is tracked by a fixed-array order-state
machine (`src/oms/order_manager.hpp`).

**Persistence.** The strategy pushes periodic `BookSnapshot` PODs and every fill into
SPSC rings; a dedicated thread drains them into LMDB using one write transaction per
batch (`src/persist/lmdb_persister.hpp`). The PODs are the serialized form, so the hot
path pays a ~200-byte memcpy into a ring and nothing else — the mmap write and commit
fsync are entirely on the cold thread.

**Metrics.** A lock-free log2-bucketed histogram (HdrHistogram-style, ~5 ns per
record, ~3% relative error) captures the two required critical flows: F1
`md_processing` (FIX callback entry → order book updated) and F2 `tick_to_trade`
(FIX callback entry → NewOrderSingle handed to the socket), plus `queue_wait` (ring
residency, which isolates backlog-induced delay). Throughput (TPS) and the high-water
mark of every internal queue are published at 1 Hz to Prometheus and to stdout.

**Operations log.** Every order, cancel and execution is logged as a POD into a ring
drained by an async CSV writer — the hot path never formats strings or touches the
filesystem. Output `ops_live.csv` is directly loadable in pandas for post-trade
strategy analysis.

## A note on GPU usage (asked during design)

We deliberately do **not** use the GPU on the trading path. A GPU kernel launch plus
PCIe round trip costs 5–20 µs in the best case — two orders of magnitude more than
this engine's entire hot path (~70–110 ns per event measured, see
`docs/ANALYSIS.md`). GPUs win on throughput for large batched workloads (pricing
thousands of options, training models, end-of-day risk), not on the latency of
deciding on a single 200-byte book update. Where data-parallelism genuinely helps at
this scale, SIMD on the CPU is the right tool, and that is what the AVX2 VWAP and the
flat-array book layout exploit. This is the answer an interviewer expects: knowing
*where not* to use the GPU is part of low-latency design.

---

## Setup on Windows + Visual Studio 2022

### 1. Prerequisites

Install, in this order:

1. **Visual Studio 2022** (17.8+) with the workload **"Desktop development with C++"**.
   In *Individual components*, make sure these are checked: *MSVC v143*, *Windows 11
   SDK*, *C++ CMake tools for Windows*, and *C++ profiling tools* (needed later for
   the performance analysis).
2. **Git for Windows** — https://git-scm.com
3. **vcpkg** (dependency manager). In a *Developer PowerShell for VS 2022*:
   ```powershell
   git clone https://github.com/microsoft/vcpkg C:\vcpkg
   C:\vcpkg\bootstrap-vcpkg.bat
   [Environment]::SetEnvironmentVariable("VCPKG_ROOT", "C:\vcpkg", "User")
   ```
   Close and reopen the terminal so `VCPKG_ROOT` is visible.
4. **Pin the vcpkg baseline** (required — without it configure fails with
   *"this vcpkg instance requires a manifest with a specified baseline"*).
   From the `mm-engine` project folder:
   ```powershell
   cd mm-engine
   & "$env:VCPKG_ROOT\vcpkg.exe" x-update-baseline --add-initial-baseline
   ```
   This writes a `builtin-baseline` field into `vcpkg.json`, pinned to the
   commit of *your* vcpkg clone — which is why it isn't hardcoded in the repo.
   Run it once and you're set. (To pick up newer library versions later:
   `git -C $env:VCPKG_ROOT pull`, then re-run the same command.)
5. **stunnel** — https://www.stunnel.org/downloads.html (the Windows installer).
   reMarkets' FIX endpoint (`fix.remarkets.primary.com.ar:9876`) is TLS; QuickFIX
   speaks plain TCP to a local stunnel that wraps the TLS.
6. **Docker Desktop** — for the Prometheus + Grafana stack.

### 2. Open and build the project

The project uses CMake presets, which VS 2022 understands natively. Dependencies
(`quickfix`, `lmdb`, `prometheus-cpp`) are declared in `vcpkg.json` and built
automatically on first configure — expect 10–20 minutes the first time.

Option A — inside the IDE:

1. *File → Open → Folder...* and select the `mm-engine` folder (do **not** look for
   a `.sln`; VS opens CMake folders directly).
2. In the toolbar's configuration dropdown pick **vs2022-release**.
3. VS runs the CMake configure (watch the *Output → CMake* pane; vcpkg builds the
   dependencies here). When it finishes, *Build → Build All* (Ctrl+Shift+B).
4. The binary lands in `build\Release\mm_engine.exe`, with `config\` copied next
   to it automatically.

> **If the first configure failed** (e.g. with the *"requires a manifest with a
> specified baseline"* error before you ran step 4 above): fix the cause, then in
> VS use *Project → Delete Cache and Reconfigure* — a stale CMake cache will
> otherwise keep replaying the old failure.

Option B — command line (same result):

```powershell
cd mm-engine
cmake --preset vs2022-release
cmake --build --preset vs2022-release
```

If configure fails with "Could not find toolchain file", `VCPKG_ROOT` is not set in
the environment VS sees — set it as a *User* variable and restart VS.

### 3. Configure credentials and session

`config/credentials.txt` must contain the demo account (already provided):

```
username=maildetestsbs22479
password=uyrufI1$
account=REM22479
target=ROFX
```

`config/fix_config.cfg` already maps `SenderCompID` to the username and points to
`127.0.0.1:8080` (the local stunnel). Note one deliberate change versus the sample
config that came with the assignment: `SocketNodelay=Y` — Nagle's algorithm batches
small writes and would add up to ~40 ms to order sends; a market maker always wants
`TCP_NODELAY`.

`config/strategy.cfg` holds `MINIMUM_SPREAD`, `ORDER_SPREAD`, `TICK_SIZE`,
`ORDER_QTY`, `BOOK_DEPTH` (1–5) and the core assignments. Verify `TICK_SIZE` against
the instrument definition in the reMarkets UI before live runs — quoting off-tick
gets orders rejected.

### 4. Run

Terminal 1 — the TLS tunnel:

```powershell
cd mm-engine\config
& "C:\Program Files (x86)\stunnel\bin\stunnel.exe" stunnel.conf
```

Terminal 2 — the monitoring stack:

```powershell
cd mm-engine\monitoring
docker compose up -d
```

Terminal 3 — the engine:

```powershell
cd mm-engine\build\Release
.\mm_engine.exe
```

You should see `LOGON FIXT.1.1:maildetestsbs22479->ROFX`, the MarketDataRequest
confirmation, and a 1 Hz metrics line. Open Grafana at http://localhost:3000
(admin/admin) — the **MM Engine** dashboard is pre-provisioned with the latency
percentiles, TPS, backlog and order-rate panels.

For the very first session, set `DRY_RUN=1` in `config/strategy.cfg`: the engine
subscribes, builds books and makes decisions but sends nothing, which lets you verify
the session and the data before quoting. reMarkets trades roughly during market hours
(~10:00–17:00 ART, Mon–Fri); outside those hours you'll log on but see no quotes, and
you can inject liquidity yourself from https://remarkets.matriz.com.ar/ to wake the
strategy up — watching your own engine tighten the spread you just opened is the most
satisfying test in this exercise.

### 5. Load test / profiling mode

```powershell
.\mm_engine.exe --bench 2000000
```

This pushes 2M synthetic full-depth snapshots (mixing wide and narrow spreads, so both
strategy branches execute) through the *exact* production path — rings, book, strategy,
order generation — without the network, then prints throughput, p50/p90/p99 for both
critical flows, and queue high-water marks. This is the binary you attach the profiler
to; see `docs/ANALYSIS.md` for the full profiling methodology and results.

### 6. Verifying persistence

LMDB data lands in `data/lmdb` (snapshots under keys `ob|<symbol>|<seq>`, fills under
`tr|<seq>`). `scripts/dump_lmdb.py` (needs `pip install lmdb`) pretty-prints both.

## Repository layout

```
src/common/     spsc_ring, object_pool, latency_histogram, timestamp, affinity,
                async_logger, events (POD types), config
src/book/       order_book (flat-array L2, AVX2 VWAP)
src/oms/        order_manager (quote state machine)
src/strategy/   market_maker (the hot loop)
src/fix/        fix_app (QuickFIX Application: session, MD parse, order send)
src/persist/    lmdb_persister (batched cold-thread writes)
src/metrics/    metrics (registry) + metrics_server (Prometheus exposer)
src/main.cpp    thread wiring, live + --bench modes
config/         fix_config.cfg, strategy.cfg, stunnel.conf, dictionaries, symbols
monitoring/     docker-compose.yml, prometheus.yml, Grafana dashboard (auto-provisioned)
docs/           ANALYSIS.md — profiling study, bottlenecks, theoretical improvements
```

## Linux build (optional)

```bash
sudo apt install build-essential cmake ninja-build stunnel4
export VCPKG_ROOT=$HOME/vcpkg   # bootstrap as on Windows
cmake --preset linux-release && cmake --build --preset linux-release
```

On Linux you additionally get `perf`, and isolating the strategy core with
`isolcpus=2,3 nohz_full=2,3` in the kernel cmdline makes the pinning fully effective.
