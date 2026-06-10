# mm-engine

A low-latency market-making engine for six DLR futures on Primary's reMarkets
simulator (ROFEX). C++17, QuickFIX over FIX 5.0 SP2, lock-free internals, LMDB
persistence, Prometheus + Grafana for metrics. Built and tested on Windows 11
with Visual Studio 2022; builds on Linux too.

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
   Strategy ──[LogRing]────────────► async CSV logger (ops_live.csv)
   Metrics thread (1 Hz) ──────────► Prometheus :9091 ──► Grafana :3000
```

## Design notes

Threading: one pipeline stage per thread, connected only by single-producer/
single-consumer lock-free rings (`src/common/spsc_ring.hpp`). No mutexes on the
hot path; the only shared writes are ring publishes and relaxed atomic
counters. The strategy thread and the sender are pinned to dedicated cores
(`STRATEGY_CORE` / `FIX_CORE` in `config/strategy.cfg`) and busy-spin with
`_mm_pause`, since a scheduler wake-up alone costs more than the whole
decision.

Memory: everything that crosses a thread boundary is a fixed-size POD
(`src/common/events.hpp`) and the rings are preallocated, so steady state does
zero heap allocations. Ring head/tail indices sit on separate cache lines with
cached copies of the opposite index, so a push or pop normally touches one
line and causes no coherence traffic. The C4324 padding warnings MSVC prints
during the build are this alignment doing its job.

Order book: ROFEX sends full snapshots only (`MDUpdateType=0`) with depth
capped at 5, so the book is just two flat arrays per side in a cache-aligned
struct. Applying a snapshot is a memcpy; best bid/ask is element zero. The
theoretical price uses the mid by default, with a depth-weighted VWAP
implemented in AVX2 FMA (`src/book/order_book.hpp`, scalar fallback included).

Strategy, as specified by the assignment: on each snapshot, if the market
spread is at least `MINIMUM_SPREAD` the engine quotes a buy at BestBid + 1 tick
and a sell at BestAsk - 1 tick; if the spread is tighter than that it cancels
everything. Quotes follow the market by cancel-then-requote. One working quote
per symbol and side is tracked by a small state machine
(`src/oms/order_manager.hpp`) so the engine never double-quotes an
unacknowledged order.

Persistence: the strategy pushes periodic book snapshots and every fill into
rings; a dedicated thread drains them into LMDB with one write transaction per
batch (`src/storage/lmdb_persister.hpp`). The hot path pays a ~200-byte copy
into a ring; the mmap writes and the commit happen on the cold thread.

Metrics: a lock-free log2-bucketed histogram (~5 ns per record) captures the
two flows the assignment asks for -- F1 `md_processing` (FIX callback entry to
book updated) and F2 `tick_to_trade` (same entry to NewOrderSingle on the
socket) -- plus `queue_wait`, which isolates ring residency from compute.
Throughput and per-queue high-water marks are published at 1 Hz to Prometheus
and echoed to stdout. Orders, cancels and executions are also logged through a
ring to an async CSV writer, so the hot path never formats a string.


## Setup (Windows + Visual Studio 2022)

### 1. Prerequisites

1. Visual Studio 2022 (17.8+) with the "Desktop development with C++"
   workload. Under Individual components also check: MSVC v143, a Windows SDK,
   C++ CMake tools for Windows, and C++ profiling tools (used for the
   performance analysis later).
2. Git for Windows: https://git-scm.com
3. vcpkg. In a Developer PowerShell for VS 2022:
   ```powershell
   git clone https://github.com/microsoft/vcpkg C:\vcpkg
   C:\vcpkg\bootstrap-vcpkg.bat
   [Environment]::SetEnvironmentVariable("VCPKG_ROOT", "C:\vcpkg", "User")
   ```
   Reopen the terminal afterwards so `VCPKG_ROOT` is visible.
4. Pin the vcpkg baseline. Without this, the first configure fails with
   "this vcpkg instance requires a manifest with a specified baseline":
   ```powershell
   cd mm-engine
   & "$env:VCPKG_ROOT\vcpkg.exe" x-update-baseline --add-initial-baseline
   ```
   This writes a `builtin-baseline` into `vcpkg.json` pinned to the commit of
   your local vcpkg clone, which is why the value is not hardcoded in the
   repo. Run it once.
5. stunnel (https://www.stunnel.org/downloads.html). reMarkets' FIX endpoint
   is TLS; QuickFIX talks plain TCP to a local stunnel that does the TLS.
6. Docker Desktop, for Prometheus + Grafana.
7. Python 3, for the helper scripts. If you also have Python 2 on PATH
   (it happens), invoke the scripts with `py -3` instead of `python`.

### 2. Build

Dependencies (quickfix, lmdb, prometheus-cpp) are declared in `vcpkg.json`
and build automatically on the first configure. That first build takes
10-20 minutes; later ones are seconds.

In the IDE: File -> Open -> Folder on the `mm-engine` directory (there is no
.sln; VS opens CMake folders directly), pick the **vs2022-release** preset in
the toolbar dropdown, wait for the configure to finish in the Output -> CMake
pane, then Build -> Build All. The binary lands in `build\Release\
mm_engine.exe` with `config\` copied next to it.

Or from the command line, same result:

```powershell
cd mm-engine
cmake --preset vs2022-release
cmake --build --preset vs2022-release
```

Two build problems worth knowing about up front:

- If a configure ever fails, fix the cause and then use Project -> Delete
  Cache and Reconfigure; a stale CMake cache will keep replaying the old
  error otherwise.
- If the built exe exits instantly with no output at all, check that all the
  vcpkg DLLs made it next to it: `dir build\Release\*.dll` should list seven
  (civetweb x2, lmdb, zlib1, libcrypto, libssl, legacy). vcpkg occasionally
  finishes building OpenSSL after the copy step has already run; the fix is
  `Copy-Item build\vcpkg_installed\x64-windows\bin\*.dll build\Release\` or
  simply rebuilding once.

### 3. Credentials and configuration

`config/credentials.txt` holds the demo account in `key=value` form (username,
password, account, target). It is intentionally not committed; copy
`credentials.txt.example` and fill in the values you were issued.

`config/fix_config.cfg` is the session for the real venue, pointing at
`127.0.0.1:8080` (the local stunnel). One deliberate change versus the sample
config shipped with the assignment: `SocketNodelay=Y`. Nagle's algorithm
batches small writes and can add tens of milliseconds to an order send, which
defeats the purpose of everything else here.

`config/strategy.cfg` holds the market parameters (`MINIMUM_SPREAD`,
`TICK_SIZE`, `ORDER_QTY`, `BOOK_DEPTH`), the core pinning, and `FIX_CFG`,
which selects the session config -- this is how you switch between the real
venue and the local mock exchange below. Check `TICK_SIZE` against the
instrument in the reMarkets UI before a live run; quoting off-tick gets
rejected. For DLR futures it is 0.5.

`DRY_RUN=1` makes the engine do everything except transmit orders. Use it for
the first session against the real venue.

### 4. Run against reMarkets

Terminal 1, the TLS tunnel. Note that the stunnel Windows build reads its
config from its install directory, so either pass an absolute path or paste
the contents of `config/stunnel.conf` into
`C:\Program Files (x86)\stunnel\config\stunnel.conf` and use the GUI's
Reload Configuration:

```powershell
& "C:\Program Files (x86)\stunnel\bin\stunnel.exe" C:\full\path\to\mm-engine\config\stunnel.conf
```

Verify it listens: `Test-NetConnection 127.0.0.1 -Port 8080` should say
TcpTestSucceeded: True.

Terminal 2, monitoring:

```powershell
cd mm-engine\monitoring
docker compose up -d
```

Terminal 3, the engine:

```powershell
cd mm-engine\build\Release
.\mm_engine.exe
```

Expect `LOGON FIXT.1.1:<user>->ROFX`, a MarketDataRequest confirmation, and a
metrics line every second. Grafana is at http://localhost:3000 (admin/admin);
the "MM Engine" dashboard is provisioned automatically. reMarkets is active
roughly 10:00-17:00 ART on weekdays; outside those hours the session logs on
but the books stay empty. During hours you can create activity yourself by
placing wide orders from https://remarkets.matriz.com.ar/ and watching the
engine quote inside them -- but log out of the web platform before starting
the engine, because it appears to hold the FIX session for the same user (see
the troubleshooting note below).

### 5. Run against the local mock exchange

`scripts/mock_exchange.py` is a small FIX simulator (pure Python, no
dependencies) that accepts the logon, streams 5-level books for all six
symbols, acks orders and cancels, and fills every fifth order. It alternates
every 6 seconds between a wide spread, where the engine should quote both
sides, and a locked book, where the engine should cancel everything -- so
both branches of the strategy run continuously. This is how the engine can be
tested end to end with no external dependency and no market hours.

1. In `build\Release\config\strategy.cfg` set
   `FIX_CFG=config/fix_config_local.cfg` and `DRY_RUN=0`.
2. Terminal 1: `py -3 scripts\mock_exchange.py`
3. Terminal 2, in `build\Release`:
   ```powershell
   Remove-Item -Recurse -Force store, log   # clear old session state; errors here are fine
   .\mm_engine.exe
   ```

The mock's console prints every NewOrderSingle and CancelRequest it receives,
so you can watch the strategy work in real time; the dashboard fills in
parallel. Set `FIX_CFG` back to `config/fix_config.cfg` for real sessions.

### 6. Load test / profiling

```powershell
.\mm_engine.exe --bench 2000000
```

Pushes two million synthetic snapshots (wide and narrow mixed) through the
production pipeline with no network, then prints throughput, the percentiles
for both flows, and queue high-water marks. This is the binary to attach the
VS profiler to. Methodology, results and the bottleneck discussion are in
`docs/ANALYSIS.md`.

### 7. Inspecting persisted data

LMDB data lands in `data/lmdb`: book snapshots under `ob|<symbol>|<seq>`,
fills under `tr|<seq>`.

```powershell
py -3 -m pip install lmdb
py -3 scripts\dump_lmdb.py data\lmdb
```

### Troubleshooting the reMarkets logon

If the logon gets no answer, or a Logout with SessionStatus (tag 1409) = 9,
work through: (a) make sure nothing else is logged in as the same user --
including the reMarkets web platform, which holds a FIX session of its own;
(b) stop everything for ten minutes so the server can reap any stale session,
then try once; (c) delete `store\` and `log\` next to the exe to clear local
sequence state. If it persists after all three, the session state is stuck on
the venue side and only Primary can reset it; the mock exchange above covers
all functional testing in the meantime. The full investigation of this exact
failure mode, with message logs, is in `docs/ANALYSIS.md` section 6.

## Repository layout

```
src/common/     spsc_ring, object_pool, latency_histogram, timestamp, affinity,
                async_logger, events (POD types), config
src/book/       order_book (flat-array L2, AVX2 VWAP)
src/oms/        order_manager (quote state machine)
src/strategy/   market_maker (the hot loop)
src/fix/        fix_app (QuickFIX Application: session, MD parse, order send)
src/storage/    lmdb_persister (batched cold-thread writes)
src/metrics/    metrics registry + Prometheus exposer
src/main.cpp    thread wiring, live and --bench modes
scripts/        mock_exchange.py, dump_lmdb.py
config/         session configs (venue + local mock), strategy.cfg,
                stunnel.conf, FIX dictionaries, symbol list
monitoring/     docker-compose, prometheus.yml, auto-provisioned Grafana dashboard
docs/           ANALYSIS.md (profiling study) and the screenshots it references
```

## Linux build (optional)

```bash
sudo apt install build-essential cmake ninja-build stunnel4
export VCPKG_ROOT=$HOME/vcpkg   # bootstrap as on Windows
cmake --preset linux-release && cmake --build --preset linux-release
```

Linux adds `perf` for profiling, and isolating the pinned cores with
`isolcpus=2,3 nohz_full=2,3` on the kernel command line makes the core
pinning fully effective.
