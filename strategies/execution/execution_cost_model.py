#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-8 | EXECUTION COST + SLIPPAGE MODEL

✔ Brokerage
✔ Exchange charges
✔ STT
✔ Slippage (bps-based)
✔ Index futures / index ETF safe
"""

from dataclasses import dataclass

@dataclass
class ExecutionCostConfig:
    brokerage_pct: float = 0.0002      # 0.02%
    slippage_bps: float = 5             # 5 basis points
    exchange_pct: float = 0.0000325     # NSE charges
    stt_pct: float = 0.0001             # STT on sell
    gst_pct: float = 0.18               # GST on brokerage + exchange

def estimate_execution_cost(
    price: float,
    qty: float,
    is_exit: bool,
    cfg: ExecutionCostConfig = ExecutionCostConfig()
) -> float:
    """
    Returns total execution cost in ₹
    """

    notional = price * qty

    brokerage = notional * cfg.brokerage_pct
    exchange = notional * cfg.exchange_pct
    gst = (brokerage + exchange) * cfg.gst_pct

    stt = notional * cfg.stt_pct if is_exit else 0.0

    slippage = notional * (cfg.slippage_bps / 10000)

    total_cost = brokerage + exchange + gst + stt + slippage
    return round(total_cost, 4)


# -----------------------------
# SELF TEST
# -----------------------------
if __name__ == "__main__":
    cost = estimate_execution_cost(price=26000, qty=50, is_exit=True)
    print("Estimated execution cost ₹:", cost)
