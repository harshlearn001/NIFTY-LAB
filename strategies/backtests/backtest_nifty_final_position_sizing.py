#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Backtest FINAL NIFTY SYSTEM with REAL POSITION SIZING
====================================================

- Initial capital based
- Fixed fractional risk
- ATR-based SL
- NIFTY futures (lot size = 75)

This produces REALISTIC, LIVE-TRADABLE results.
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
# PARAMETERS (LIVE-READY)
# =================================================
INITIAL_CAPITAL = 1_000_000      # ₹10,00,000
RISK_PCT        = 0.01           # 1% risk per trade
LOT_SIZE        = 75             # NIFTY lot size
MAX_LOTS        = 10             # safety cap

# =================================================
# PATHS
# =================================================
DATA_FILE = PROC_DIR / "futures_ml" / "nifty_final_trade_signals.parquet"
OUT_FILE  = PROC_DIR / "futures_ml" / "backtest_nifty_final_position_sizing.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading final strategy signals...")
df = pd.read_parquet(DATA_FILE).sort_values("date").reset_index(drop=True)

capital = INITIAL_CAPITAL
equity_curve = []
trades = []

# =================================================
# BACKTEST LOOP
# =================================================
for i in range(len(df) - 1):
    row = df.iloc[i]

    equity_curve.append({
        "date": row["date"],
        "capital": capital
    })

    if row["final_signal"] == "NO_TRADE":
        continue

    entry = row["close"]
    sl = row["stop_loss"]
    tgt = row["target"]

    if pd.isna(sl) or pd.isna(tgt):
        continue

    sl_points = abs(entry - sl)
    if sl_points <= 0:
        continue

    # ---- POSITION SIZING ----
    risk_amt = capital * RISK_PCT
    lots = int(risk_amt // (sl_points * LOT_SIZE))
    lots = min(lots, MAX_LOTS)

    if lots <= 0:
        continue

    next_row = df.iloc[i + 1]
    exit_price = next_row["close"]

    # ---- EXIT LOGIC ----
    if row["final_signal"] == "BUY":
        if next_row["close"] <= sl:
            exit_price = sl
        elif next_row["close"] >= tgt:
            exit_price = tgt

        pnl = (exit_price - entry) * LOT_SIZE * lots

    else:  # SELL
        if next_row["close"] >= sl:
            exit_price = sl
        elif next_row["close"] <= tgt:
            exit_price = tgt

        pnl = (entry - exit_price) * LOT_SIZE * lots

    capital += pnl

    trades.append({
        "date": row["date"],
        "signal": row["final_signal"],
        "lots": lots,
        "entry": entry,
        "exit": exit_price,
        "pnl": pnl,
        "capital_after": capital,
    })

# =================================================
# RESULTS
# =================================================
bt = pd.DataFrame(trades)
eq = pd.DataFrame(equity_curve)

if bt.empty:
    raise ValueError("❌ No trades executed — reduce SL or increase capital")

total_trades = len(bt)
win_rate = (bt["pnl"] > 0).mean() * 100
avg_pnl = bt["pnl"].mean()
final_capital = capital

eq["peak"] = eq["capital"].cummax()
eq["drawdown_pct"] = (eq["capital"] - eq["peak"]) / eq["peak"] * 100
max_dd = eq["drawdown_pct"].min()

years = (eq["date"].iloc[-1] - eq["date"].iloc[0]).days / 365
cagr = ((final_capital / INITIAL_CAPITAL) ** (1 / years) - 1) * 100

# =================================================
# SAVE
# =================================================
bt.to_parquet(OUT_FILE, index=False)

# =================================================
# SUMMARY
# =================================================
print("\n📊 REAL CAPITAL BACKTEST RESULTS (LIVE-READY)")
print(f"Initial Capital : ₹{INITIAL_CAPITAL:,.0f}")
print(f"Final Capital   : ₹{final_capital:,.0f}")
print(f"Total Trades    : {total_trades}")
print(f"Win Rate (%)    : {win_rate:.2f}")
print(f"Avg PnL / trade : ₹{avg_pnl:,.0f}")
print(f"CAGR (%)        : {cagr:.2f}")
print(f"Max Drawdown %  : {max_dd:.2f}")
print(f"\n📦 Saved: {OUT_FILE}")
