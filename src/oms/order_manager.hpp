#pragma once
// ----------------------------------------------------------------------------
// OrderManager — minimal order-state machine owned by the STRATEGY thread.
// One working quote per (symbol, side). Fixed arrays, no allocation.
// ----------------------------------------------------------------------------
#include "../common/events.hpp"
#include <cstring>

enum class QuoteState : uint8_t { None=0, PendingNew, Live, PendingCancel };

struct Quote {
    QuoteState state = QuoteState::None;
    uint64_t   cl_ord_id = 0;
    double     price = 0;
    double     qty = 0;
    char       exch_order_id[24] = {};
};

class OrderManager {
public:
    Quote& quote(uint32_t sym, OrdSide side) noexcept {
        return q_[sym][side == OrdSide::Buy ? 0 : 1];
    }

    // Apply an execution report event coming back from the exchange.
    void on_exec(const ExecEvent& e) noexcept {
        for (int s = 0; s < 2; ++s) {
            Quote& q = q_[e.symbol_id][s];
            if (q.cl_ord_id != e.cl_ord_id) continue;
            switch (e.kind) {
                case ExecKind::New:
                    q.state = QuoteState::Live;
                    std::memcpy(q.exch_order_id, e.order_id, sizeof(q.exch_order_id));
                    break;
                case ExecKind::PartFill:
                    q.qty = e.leaves_qty;
                    break;
                case ExecKind::Filled:
                case ExecKind::Canceled:
                case ExecKind::Rejected:
                    q = Quote{};       // slot free again
                    break;
                default: break;
            }
            return;
        }
    }

    bool any_active(uint32_t sym) const noexcept {
        return q_[sym][0].state != QuoteState::None || q_[sym][1].state != QuoteState::None;
    }

private:
    Quote q_[kMaxSymbols][2]{};
};
