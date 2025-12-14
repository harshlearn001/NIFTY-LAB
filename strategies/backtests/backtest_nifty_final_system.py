#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest FINAL NIFTY OI + PCR + ML Strategy
==========================================

Input:
- data/processed/futures_ml/nifty_final_trade_signals.parquet

Output:
- Performance stats + trade log
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
import numpy as np
from configs.paths import PROC_DIR

# =================================================
# PATHS
# =================================================
DATA_FILE = PROC_DIR / "futures_ml" / "nifty_final_trade_signals.parquet"
OUT_FILE  = PROC_DIR / "futures_ml" / "backtest_nifty_final_system.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading final strategy signals...")
df = pd.read_parquet(DATA_FILE).sort_values("date").reset_index(drop=True)

# =================================================
# BACKTEST ENGINE
# =================================================
trades = []

for i in range(len(df) - 1):
    row = df.iloc[i]

    if row["final_signal"] == "NO_TRADE":
        continue

    entry = row["close"]
    next_row = df.iloc[i + 1]

    if row["final_signal"] == "BUY":
        # Next day movement
        exit_price = next_row["close"]

        # Check SL / TARGET
        if next_row["close"] <= row["stop_loss"]:
            exit_price = row["stop_loss"]
        elif next_row["close"] >= row["target"]:
            exit_price = row["target"]

        ret = (exit_price - entry) / entry

    else:  # SELL
        exit_price = next_row["close"]

        if next_row["close"] >= row["stop_loss"]:
            exit_price = row["stop_loss"]
        elif next_row["close"] <= row["target"]:
            exit_price = row["target"]

        ret = (entry - exit_price) / entry

    trades.append({
        "date": row["date"],
        "signal": row["final_signal"],
        "entry": entry,
        "exit": exit_price,
        "return": ret,
    })

# =================================================
# RESULTS
# =================================================
bt = pd.DataFrame(trades)

if bt.empty:
    raise ValueError("❌ No trades generated in backtest")

bt["cum_return"] = (1 + bt["return"]).cumprod()

total_trades = len(bt)
win_rate = (bt["return"] > 0).mean() * 100
avg_trade = bt["return"].mean() * 100
cagr = (bt["cum_return"].iloc[-1] ** (252 / total_trades) - 1) * 100
max_dd = ((bt["cum_return"] / bt["cum_return"].cummax()) - 1).min() * 100

# =================================================
# SAVE
# =================================================
bt.to_parquet(OUT_FILE, index=False)

# =================================================
# SUMMARY
# =================================================
print("\n📊 BACKTEST RESULTS — FINAL SYSTEM")
print(f"Total trades      : {total_trades}")
print(f"Win rate (%)      : {win_rate:.2f}")
print(f"Average trade (%) : {avg_trade:.3f}")
print(f"CAGR (%)          : {cagr:.2f}")
print(f"Max drawdown (%)  : {max_dd:.2f}")
print(f"\n📦 Saved: {OUT_FILE}")
