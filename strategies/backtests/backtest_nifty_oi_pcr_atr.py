#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest: NIFTY OI + PCR Strategy with ATR Risk Management
---------------------------------------------------------
- Entry: Close on signal day
- Exit : Next day close OR ATR stop
- Risk : 1% of equity per trade
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
SIGNAL_FILE = PROC_DIR / "futures_ml" / "nifty_oi_pcr_trade_signals.parquet"
OUT_FILE = PROC_DIR / "futures_ml" / "backtest_nifty_oi_pcr_atr.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading strategy signals...")

df = pd.read_parquet(SIGNAL_FILE).sort_values("date").reset_index(drop=True)

# =================================================
# ATR CALCULATION (14)
# =================================================
df["prev_close"] = df["close"].shift(1)
df["tr"] = abs(df["close"] - df["prev_close"])
df["atr"] = df["tr"].rolling(14).mean()

# =================================================
# BACKTEST SETTINGS
# =================================================
equity = 1.0
risk_pct = 0.01  # 1% risk per trade

equity_curve = []
trade_returns = []

# =================================================
# BACKTEST LOOP
# =================================================
for i in range(len(df) - 1):
    row = df.iloc[i]

    equity_curve.append(equity)

    if row["final_signal"] == "NO_TRADE" or pd.isna(row["atr"]):
        continue

    entry = row["close"]
    exit_price = df.iloc[i + 1]["close"]

    stop_dist = row["atr"]
    if stop_dist <= 0:
        continue

    # Position size based on risk
    position_size = (equity * risk_pct) / (stop_dist / entry)

    # Raw return
    ret = (exit_price - entry) / entry
    if row["final_signal"] == "SELL":
        ret *= -1

    # Apply ATR stop
    if ret < -(stop_dist / entry):
        ret = -(stop_dist / entry)

    trade_pnl = position_size * ret
    equity += trade_pnl

    trade_returns.append(trade_pnl)

# Append last equity
equity_curve.append(equity)

df_bt = df.iloc[:len(equity_curve)].copy()
df_bt["equity"] = equity_curve

# =================================================
# METRICS
# =================================================
trades = np.array(trade_returns)

total_trades = len(trades)
win_rate = (trades > 0).mean() * 100 if total_trades else 0
avg_trade = trades.mean() * 100 if total_trades else 0

cum_max = df_bt["equity"].cummax()
drawdown = (df_bt["equity"] / cum_max - 1)
max_dd = drawdown.min() * 100

days = len(df_bt)
cagr = df_bt["equity"].iloc[-1] ** (252 / days) - 1

# =================================================
# RESULTS
# =================================================
print("\n📊 BACKTEST RESULTS (ATR + POSITION SIZING)")
print(f"Total trades      : {total_trades}")
print(f"Win rate (%)      : {win_rate:.2f}")
print(f"Average trade (%) : {avg_trade:.3f}")
print(f"CAGR (%)          : {cagr * 100:.2f}")
print(f"Max drawdown (%)  : {max_dd:.2f}")

# =================================================
# SAVE
# =================================================
df_bt.to_parquet(OUT_FILE, index=False)
print(f"\n📦 Saved: {OUT_FILE}")
