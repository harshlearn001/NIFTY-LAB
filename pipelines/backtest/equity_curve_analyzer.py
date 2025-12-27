#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | EQUITY CURVE ANALYZER (OPTIONS)

✔ Uses canonical PnL history file
✔ Capital curve
✔ Drawdown curve
✔ Max DD
✔ Win rate
✔ No side effects
"""

from pathlib import Path
import pandas as pd
import numpy as np

# ==================================================
# PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[2]

PNL_FILE = ROOT / "data" / "backtest" / "nifty_option_pnl_history.csv"
OUT_DIR  = ROOT / "data" / "analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

EQUITY_CURVE_FILE = OUT_DIR / "nifty_equity_curve.csv"
DD_FILE           = OUT_DIR / "nifty_drawdown_curve.csv"

# ==================================================
# LOAD
# ==================================================
if not PNL_FILE.exists():
    raise FileNotFoundError(f"❌ PnL file not found: {PNL_FILE}")

df = pd.read_csv(PNL_FILE, parse_dates=["DATE"])

# ==================================================
# EQUITY CURVE
# ==================================================
df = df.sort_values("DATE").reset_index(drop=True)

df["EQUITY"] = df["CAPITAL"]
df["RET"] = df["EQUITY"].pct_change().fillna(0)

# ==================================================
# DRAWDOWN
# ==================================================
df["PEAK"] = df["EQUITY"].cummax()
df["DRAWDOWN"] = (df["EQUITY"] - df["PEAK"]) / df["PEAK"]

max_dd = df["DRAWDOWN"].min()

# ==================================================
# STATS
# ==================================================
trades = df[df["ACTION"] != "HOLD"]

win_rate = (trades["PNL"] > 0).mean() * 100 if len(trades) else 0
total_pnl = trades["PNL"].sum()

# ==================================================
# SAVE OUTPUTS
# ==================================================
df[["DATE", "EQUITY"]].to_csv(EQUITY_CURVE_FILE, index=False)
df[["DATE", "DRAWDOWN"]].to_csv(DD_FILE, index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n📈 EQUITY CURVE ANALYSIS COMPLETE")
print(f"📁 Equity curve  : {EQUITY_CURVE_FILE}")
print(f"📁 Drawdown curve: {DD_FILE}")
print(f"💰 Total PnL     : {total_pnl:,.2f}")
print(f"📉 Max Drawdown  : {max_dd:.2%}")
print(f"🎯 Win Rate      : {win_rate:.2f}%")
print(f"🔢 Trades        : {len(trades)}")
