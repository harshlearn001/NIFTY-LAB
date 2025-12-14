#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest: NIFTY OI + PCR Strategy
ATR Stop + Confidence-Based Sizing + 2-Day Hold for Winners
-----------------------------------------------------------
- Entry: Close on signal day
- Exit :
    * If Day-1 favorable → exit Day-2 close
    * Else → exit Day-1 close
    * ATR stop always active
- Risk :
    * 0.5% / 1.0% / 1.5% based on confidence
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
OUT_FILE = PROC_DIR / "futures_ml" / "backtest_nifty_oi_pcr_atr_conf_2day.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading strategy signals...")
df = pd.read_parquet(SIGNAL_FILE).sort_values("date").reset_index(drop=True)

# =================================================
# ATR (14)
# =================================================
df["prev_close"] = df["close"].shift(1)
df["tr"] = (df["close"] - df["prev_close"]).abs()
df["atr"] = df["tr"].rolling(14).mean()

# =================================================
# CONFIDENCE → RISK
# =================================================
def risk_from_conf(conf):
    if conf > 44:
        return 0.015
    if conf >= 38:
        return 0.010
    return 0.005

df["risk_pct"] = df["confidence"].apply(risk_from_conf)

# =================================================
# BACKTEST LOOP
# =================================================
equity = 1.0
equity_curve = []
trade_pnls = []

for i in range(len(df) - 2):  # need i+2 for 2-day hold
    row = df.iloc[i]
    equity_curve.append(equity)

    if row["final_signal"] == "NO_TRADE" or pd.isna(row["atr"]):
        continue

    entry = row["close"]
    close_d1 = df.iloc[i + 1]["close"]
    close_d2 = df.iloc[i + 2]["close"]

    stop_dist = row["atr"]
    if stop_dist <= 0:
        continue

    # Position size
    risk_pct = row["risk_pct"]
    position_size = (equity * risk_pct) / (stop_dist / entry)

    # Day-1 return
    ret_d1 = (close_d1 - entry) / entry
    if row["final_signal"] == "SELL":
        ret_d1 *= -1

    # ATR stop check (day-1)
    max_loss = stop_dist / entry
    if ret_d1 < -max_loss:
        ret = -max_loss
    else:
        # Winner? Hold to day-2
        if ret_d1 > 0:
            ret_d2 = (close_d2 - entry) / entry
            if row["final_signal"] == "SELL":
                ret_d2 *= -1
            # Apply ATR stop over 2 days
            if ret_d2 < -max_loss:
                ret = -max_loss
            else:
                ret = ret_d2
        else:
            # Loser or flat → exit day-1
            ret = ret_d1

    trade_pnl = position_size * ret
    equity += trade_pnl
    trade_pnls.append(trade_pnl)

# Append last equity
equity_curve.append(equity)

df_bt = df.iloc[:len(equity_curve)].copy()
df_bt["equity"] = equity_curve

# =================================================
# METRICS
# =================================================
trades = np.array(trade_pnls)
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
print("\n📊 BACKTEST RESULTS (2-DAY HOLD FOR WINNERS)")
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
