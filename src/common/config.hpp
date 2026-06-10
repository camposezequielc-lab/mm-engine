#pragma once
// Minimal key=value config (loaded once at startup — not on the hot path).
#include <string>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <vector>

struct EngineConfig {
    // strategy
    double minimum_spread = 0.05;   // MINIMUM_SPREAD from the assignment
    double order_spread   = 0.02;   // ORDER_SPREAD (informative; aggressive mode uses ticks)
    double tick_size      = 0.001;  // DLR futures price tick (verify via SecurityList)
    double order_qty      = 1.0;
    int    book_depth     = 5;      // configurable 1..5
    // session
    std::string account   = "REM22479";
    std::string sender    = "";
    std::string fix_cfg   = "config/fix_config.cfg";
    std::string symbols_file = "config/Actives.txt";
    // infra
    int strategy_core = 2;
    int fix_core      = 3;
    int metrics_port  = 9091;
    std::string lmdb_path = "data/lmdb";
    int snapshot_interval_ms = 1000;
    bool dry_run = false;           // if true: decide but don't send orders

    std::vector<std::string> symbols;

    static EngineConfig load(const std::string& path) {
        EngineConfig c;
        std::ifstream f(path);
        std::string line;
        std::unordered_map<std::string, std::string> kv;
        while (std::getline(f, line)) {
            if (line.empty() || line[0] == '#') continue;
            auto eq = line.find('=');
            if (eq == std::string::npos) continue;
            auto k = line.substr(0, eq), v = line.substr(eq + 1);
            while (!v.empty() && (v.back()=='\r'||v.back()==' ')) v.pop_back();
            kv[k] = v;
        }
        auto get = [&](const char* k, auto def) {
            using T = decltype(def);
            auto it = kv.find(k);
            if (it == kv.end()) return def;
            if constexpr (std::is_same_v<T, double>) return std::stod(it->second);
            else if constexpr (std::is_same_v<T, int>) return std::stoi(it->second);
            else if constexpr (std::is_same_v<T, bool>) return it->second == "1" || it->second == "true";
            else return it->second;
        };
        c.minimum_spread = get("MINIMUM_SPREAD", c.minimum_spread);
        c.order_spread   = get("ORDER_SPREAD",   c.order_spread);
        c.tick_size      = get("TICK_SIZE",      c.tick_size);
        c.order_qty      = get("ORDER_QTY",      c.order_qty);
        c.book_depth     = get("BOOK_DEPTH",     c.book_depth);
        c.account        = get("ACCOUNT",        c.account);
        c.fix_cfg        = get("FIX_CFG",        c.fix_cfg);
        c.symbols_file   = get("SYMBOLS_FILE",   c.symbols_file);
        c.strategy_core  = get("STRATEGY_CORE",  c.strategy_core);
        c.fix_core       = get("FIX_CORE",       c.fix_core);
        c.metrics_port   = get("METRICS_PORT",   c.metrics_port);
        c.lmdb_path      = get("LMDB_PATH",      c.lmdb_path);
        c.snapshot_interval_ms = get("SNAPSHOT_INTERVAL_MS", c.snapshot_interval_ms);
        c.dry_run        = get("DRY_RUN",        c.dry_run);

        std::ifstream sf(c.symbols_file);
        while (std::getline(sf, line)) {
            while (!line.empty() && (line.back()=='\r'||line.back()==' ')) line.pop_back();
            if (line.empty() || line[0] == '#') continue;
            c.symbols.push_back(line);
        }
        return c;
    }
};
