#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-9 | MONTE CARLO SIMULATION (PRODUCTION SAFE)

✔ Uses real backtest trade PnL distribution
✔ Equity curve robustness testing
✔ Drawdown & ruin probability
✔ Scheduler + CLI safe
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# ==================================================
# BOOTSTRAP (CRITICAL)
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR

print("🧪 PHASE-9 | MONTE CARLO SIMULATION")

# ==================================================
# CONFIG
# ==================================================
N_SIMULATIONS   = 2000
INITIAL_EQUITY  = 1.0

# ==================================================
# LOAD BACKTEST RESULTS
# ==================================================
BT_FILE = BASE_DIR / "data/backtest_nifty_results.csv"

if not BT_FILE.exists():
    raise FileNotFoundError(f"Backtest file not found → {BT_FILE}")

bt = pd.read_csv(BT_FILE)

if "pnl" not in bt.columns:
    raise ValueError("Column 'pnl' missing in backtest results")

pnl_series = bt["pnl"].dropna().values

if len(pnl_series) < 50:
    raise ValueError("Not enough trades for Monte Carlo simulation")

print(f"📊 Trades used : {len(pnl_series)}")

# ==================================================
# MONTE CARLO ENGINE
# ==================================================
def run_simulation(pnls: np.ndarray):
    equity = INITIAL_EQUITY
    peak = equity
    max_dd = 0.0

    for pnl in np.random.choice(pnls, size=len(pnls), replace=True):
        equity += pnl
        peak = max(peak, equity)
        dd = (equity / peak) - 1
        max_dd = min(max_dd, dd)

    return equity, max_dd


results = [run_simulation(pnl_series) for _ in range(N_SIMULATIONS)]

final_equity = np.array([r[0] for r in results])
max_dd       = np.array([r[1] for r in results])

# ==================================================
# SUMMARY METRICS
# ==================================================
summary = {
    "simulations": N_SIMULATIONS,
    "mean_final_equity": round(final_equity.mean(), 3),
    "median_final_equity": round(np.median(final_equity), 3),
    "best_equity": round(final_equity.max(), 3),
    "worst_equity": round(final_equity.min(), 3),
    "prob_equity_below_1": round((final_equity < 1).mean(), 3),
    "avg_max_drawdown": round(max_dd.mean(), 3),
    "worst_drawdown": round(max_dd.min(), 3),
}

out = pd.DataFrame([summary])

# ==================================================
# SAVE OUTPUT
# ==================================================
OUT_FILE = BASE_DIR / "data/analysis/monte_carlo_summary.csv"
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

out.to_csv(OUT_FILE, index=False)

print("\n✅ MONTE CARLO SIMULATION COMPLETE")
print(out)
print(f"\n💾 Saved → {OUT_FILE}")
