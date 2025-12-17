#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FUTURES OI HISTORICAL ANALYTICS
Builds front-month OI regime history (2016 → today)
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")

FUT_MASTER = BASE / "data" / "continuous" / "master_futures.parquet"
OUT_DIR = BASE / "data" / "processed" / "futures_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ = OUT_DIR / "nifty_fut_oi_historical.parquet"

print("📥 Loading futures master...")
df = pd.read_parquet(FUT_MASTER)

df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"])

# Front-month selection
front = (
    df.sort_values("EXP_DATE")
      .groupby("TRADE_DATE", as_index=False)
      .first()
      .sort_values("TRADE_DATE")
)

# OI + price deltas
front["price"] = front["CLOSE"]
front["oi"] = front["OI"]

front["price_change"] = front["price"].diff()
front["price_pct_change"] = front["price"].pct_change()
front["oi_change"] = front["oi"].diff()
front["oi_pct_change"] = front["oi"].pct_change()

def classify(r):
    if r.price_change > 0 and r.oi_change > 0:
        return "LONG_BUILDUP"
    if r.price_change < 0 and r.oi_change > 0:
        return "SHORT_BUILDUP"
    if r.price_change > 0 and r.oi_change < 0:
        return "SHORT_COVERING"
    if r.price_change < 0 and r.oi_change < 0:
        return "LONG_UNWINDING"
    return "NEUTRAL"

front["oi_signal"] = front.apply(classify, axis=1)

out = front[[
    "TRADE_DATE",
    "price_pct_change",
    "oi_pct_change",
    "oi_signal"
]].dropna()

out.to_parquet(OUT_PQ, index=False)

print("✅ FUTURES OI HISTORY READY")
print(f"📊 Rows: {len(out):,}")
print(f"📅 From: {out.TRADE_DATE.min().date()} → {out.TRADE_DATE.max().date()}")
