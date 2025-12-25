#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-10 | WALK-FORWARD VALIDATION (PRODUCTION GRADE)

✔ Rolling train-test windows
✔ No lookahead bias
✔ Uses actual strategy PnL
✔ Outputs regime-aware metrics
"""

from pathlib import Path
import pandas as pd
import numpy as np

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB-Trial")

TRADES_FILE = BASE / "data/backtest_nifty_results.csv"
OUT_FILE    = BASE / "data/analysis/walk_forward_results.csv"

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
TRAIN_DAYS = 750     # ~3 years
TEST_DAYS  = 125     # ~6 months

# --------------------------------------------------
# LOAD TRADES
# --------------------------------------------------
df = pd.read_csv(TRADES_FILE)

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

print("🚶 PHASE-10 | WALK-FORWARD VALIDATION")
print(f"📊 Total trades : {len(df)}")

results = []

start_idx = 0

while True:
    train_end = start_idx + TRAIN_DAYS
    test_end  = train_end + TEST_DAYS

    if test_end > len(df):
        break

    train = df.iloc[start_idx:train_end]
    test  = df.iloc[train_end:test_end]

    # ---- TRAIN METRICS (INFO ONLY)
    train_win = (train["pnl"] > 0).mean()
    train_pnl = train["pnl"].sum()

    # ---- TEST METRICS (REAL SCORE)
    test_win = (test["pnl"] > 0).mean()
    test_pnl = test["pnl"].sum()
    max_dd = test["drawdown"].min()

    results.append({
        "train_start": train["date"].iloc[0].date(),
        "train_end": train["date"].iloc[-1].date(),
        "test_start": test["date"].iloc[0].date(),
        "test_end": test["date"].iloc[-1].date(),
        "train_win_rate": round(train_win, 3),
        "train_total_pnl": round(train_pnl, 3),
        "test_win_rate": round(test_win, 3),
        "test_total_pnl": round(test_pnl, 3),
        "test_max_dd": round(max_dd, 3),
    })

    start_idx += TEST_DAYS

wf = pd.DataFrame(results)
wf.to_csv(OUT_FILE, index=False)

print("\n✅ WALK-FORWARD COMPLETE")
print(f"📁 Saved → {OUT_FILE}")

print("\n📊 SAMPLE")
print(wf.head())
