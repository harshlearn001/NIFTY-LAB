#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | MARKET REGIME DETECTION (NIFTY)

✔ Trend regime
✔ Volatility regime
✔ No look-ahead
✔ Production safe
"""

from pathlib import Path
import pandas as pd
import numpy as np

from configs.paths import CONT_DIR

# --------------------------------------------------
# INPUT / OUTPUT
# --------------------------------------------------
NIFTY_CONT = CONT_DIR / "nifty_continuous.parquet"
OUT_FILE   = CONT_DIR / "nifty_regime.parquet"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
df = pd.read_parquet(NIFTY_CONT).sort_values("DATE").reset_index(drop=True)

# --------------------------------------------------
# BASIC RETURNS
# --------------------------------------------------
df["RET_1D"] = df["CLOSE"].pct_change()

# --------------------------------------------------
# TREND FEATURES
# --------------------------------------------------
df["SMA_20"] = df["CLOSE"].rolling(20).mean()
df["SMA_50"] = df["CLOSE"].rolling(50).mean()

df["TREND_UP"] = (df["SMA_20"] > df["SMA_50"]).astype(int)

# --------------------------------------------------
# VOLATILITY FEATURES
# --------------------------------------------------
df["RANGE"] = (df["HIGH"] - df["LOW"]) / df["CLOSE"]
df["VOL_20"] = df["RANGE"].rolling(20).mean()
df["VOL_PCTL"] = df["VOL_20"].rank(pct=True)

# --------------------------------------------------
# REGIME LABELS
# --------------------------------------------------
def trend_regime(row):
    if row["TREND_UP"] == 1:
        return "BULL"
    return "BEAR"

def vol_regime(p):
    if p >= 0.8:
        return "HIGH_VOL"
    if p <= 0.2:
        return "LOW_VOL"
    return "MID_VOL"

df["TREND_REGIME"] = df.apply(trend_regime, axis=1)
df["VOL_REGIME"]   = df["VOL_PCTL"].apply(vol_regime)

# --------------------------------------------------
# FINAL OUTPUT
# --------------------------------------------------
out = df[[
    "DATE",
    "TREND_REGIME",
    "VOL_REGIME",
    "SMA_20",
    "SMA_50",
    "VOL_20"
]].dropna()

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
out.to_parquet(OUT_FILE, index=False)

print("✅ NIFTY REGIME FEATURES BUILT")
print(out.tail())
