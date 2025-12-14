#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest: NIFTY Futures OI + PCR Strategy
----------------------------------------
Entry: Close on signal day
Exit : Close next trading day
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # H:\NIFTY-LAB
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
SIGNAL_FILE = PROC_DIR / "futures_ml" / "nifty_oi_pcr_trade_signals.parquet"
OUT_FILE = PROC_DIR / "futures_ml" / "backtest_nifty_oi_pcr.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading final strategy signals...")

df = pd.read_parquet(SIGNAL_FILE).sort_values("date").reset_index(drop=True)

# =================================================
# BUILD RETURNS
# =================================================
df["next_close"] = df["close"].shift(-1)
df["ret"] = (df["next_close"] - df["close"]) / df["close"]

df.loc[df["final_signal"] == "SELL", "ret"] *= -1
df.loc[df["final_signal"] == "NO_TRADE", "ret"] = 0

df = df.dropna(subset=["ret"])

# =================================================
# EQUITY CURVE
# =================================================
df["equity"] = (1 + df["ret"]).cumprod()

# =================================================
# PERFORMANCE METRICS
# =================================================
trades = df[df["final_signal"] != "NO_TRADE"].copy()

total_trades = len(trades)
win_rate = (trades["ret"] > 0).mean() * 100 if total_trades > 0 else 0
avg_ret = trades["ret"].mean() * 100 if total_trades > 0 else 0

# Max Drawdown
cum_max = df["equity"].cummax()
drawdown = (df["equity"] / cum_max - 1)
max_dd = drawdown.min() * 100

# CAGR (assuming ~252 trading days)
days = len(df)
cagr = (df["equity"].iloc[-1]) ** (252 / days) - 1

# =================================================
# PRINT RESULTS
# =================================================
print("\n📊 BACKTEST RESULTS (OI + PCR STRATEGY)")
print(f"Total trades      : {total_trades}")
print(f"Win rate (%)      : {win_rate:.2f}")
print(f"Average trade (%) : {avg_ret:.3f}")
print(f"CAGR (%)          : {cagr * 100:.2f}")
print(f"Max drawdown (%)  : {max_dd:.2f}")

# =================================================
# SAVE OUTPUT
# =================================================
df.to_parquet(OUT_FILE, index=False)

print(f"\n📦 Backtest data saved to: {OUT_FILE}")
