#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-7.2 | REGIME PERFORMANCE ANALYSIS

✔ Aggregates trade attribution by market regime
✔ Computes win rate, avg pnl, max drawdown
✔ Produces TRADE / REDUCE / BAN verdict
✔ Production & research safe
"""

from pathlib import Path
import pandas as pd
import numpy as np

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB-Trial")

ATTR_FILE = BASE / "data" / "analysis" / "trade_attribution.csv"
OUT_FILE  = BASE / "data" / "analysis" / "regime_performance.csv"

print("📊 PHASE-7.2 | REGIME PERFORMANCE BUILD")

if not ATTR_FILE.exists():
    raise FileNotFoundError(f"Missing trade attribution file: {ATTR_FILE}")

df = pd.read_csv(ATTR_FILE)

# --------------------------------------------------
# BASIC CHECKS
# --------------------------------------------------
required_cols = [
    "trend_regime", "vol_regime", "pnl"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise RuntimeError(f"Missing columns: {missing}")

# --------------------------------------------------
# PERFORMANCE AGGREGATION
# --------------------------------------------------
def max_drawdown(pnl_series: pd.Series) -> float:
    equity = pnl_series.cumsum()
    peak = equity.cummax()
    dd = (equity - peak)
    return float(dd.min())

grouped = (
    df
    .groupby(["trend_regime", "vol_regime"], dropna=False)
    .apply(lambda g: pd.Series({
        "trades": len(g),
        "win_rate": (g["pnl"] > 0).mean(),
        "avg_pnl": g["pnl"].mean(),
        "total_pnl": g["pnl"].sum(),
        "max_dd": max_drawdown(g["pnl"])
    }))
    .reset_index()
)

# --------------------------------------------------
# VERDICT ENGINE (INSTITUTIONAL LOGIC)
# --------------------------------------------------
def verdict(row):
    if row["trades"] < 30:
        return "INSUFFICIENT_DATA"

    if row["win_rate"] >= 0.55 and row["avg_pnl"] > 0 and row["max_dd"] > -0.08:
        return "TRADE"

    if row["win_rate"] >= 0.48 and row["avg_pnl"] >= 0:
        return "REDUCE"

    return "BAN"

grouped["verdict"] = grouped.apply(verdict, axis=1)

# --------------------------------------------------
# SORT FOR READABILITY
# --------------------------------------------------
grouped = grouped.sort_values(
    by=["verdict", "total_pnl"],
    ascending=[True, False]
)

# --------------------------------------------------
# SAVE
# --------------------------------------------------
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
grouped.to_csv(OUT_FILE, index=False)

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
print("✅ REGIME PERFORMANCE READY")
print(f"📁 Saved → {OUT_FILE}\n")

print("📊 SAMPLE")
print(grouped.head(10))
