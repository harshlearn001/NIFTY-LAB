#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CANONICAL ML FEATURE BUILDER (FINAL)
ML uses ONLY Price + OI (NO PCR, NO Regime)
"""

import pandas as pd
import numpy as np
from configs.paths import BASE_DIR, CONT_DIR

print("🚀 Building CANONICAL ML features (ML-only)")

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def norm(df):
    for c in ["date", "DATE", "TradeDate", "TRADE_DATE"]:
        if c in df.columns:
            df["date"] = pd.to_datetime(df[c])
            return df
    raise RuntimeError("No date column found")

# --------------------------------------------------
# LOAD
# --------------------------------------------------
price = norm(pd.read_parquet(CONT_DIR / "nifty_continuous.parquet"))
oi    = norm(pd.read_parquet(BASE_DIR / "data/processed/futures_ml/nifty_fut_oi_daily.parquet"))

# --------------------------------------------------
# ALIGN DATE RANGE (PRICE ∩ OI)
# --------------------------------------------------
min_date = max(price["date"].min(), oi["date"].min())
price = price[price["date"] >= min_date].copy()
oi    = oi[oi["date"] >= min_date].copy()

# --------------------------------------------------
# PRICE FEATURES
# --------------------------------------------------
df = price[["date", "OPEN", "HIGH", "LOW", "CLOSE"]].copy()
df.rename(columns={"CLOSE": "close"}, inplace=True)

df["ret_1d"] = df["close"].pct_change()
df["ret_3d"] = df["close"].pct_change(3)

tr = np.maximum(
    df["HIGH"] - df["LOW"],
    np.maximum(
        (df["HIGH"] - df["close"].shift()).abs(),
        (df["LOW"] - df["close"].shift()).abs(),
    ),
)
df["atr_pct"] = tr.rolling(14).mean() / df["close"]
df["trend_up"] = (df["close"] > df["close"].rolling(20).mean()).astype(int)

# --------------------------------------------------
# OI FEATURES
# --------------------------------------------------
oi = oi.sort_values("date")
oi["oi_change_pct"] = oi["oi_change"] / oi["oi"].shift(1)

df = df.merge(
    oi[["date", "oi_change_pct", "regime"]],
    on="date",
    how="left",
)

# --------------------------------------------------
# OI REGIME ONE-HOT (MODEL ALIGNED)
# --------------------------------------------------
dummies = pd.get_dummies(df["regime"], prefix="regime")

for col in [
    "regime_LONG_BUILDUP",
    "regime_SHORT_COVERING",
    "regime_NO_DATA",
]:
    if col not in dummies.columns:
        dummies[col] = 0

df = pd.concat([df, dummies], axis=1)
df.drop(columns=["regime"], inplace=True)

# --------------------------------------------------
# TARGET
# --------------------------------------------------
df["next_ret"] = df["close"].pct_change().shift(-1)
df["target"] = (df["next_ret"] > 0).astype(int)

# --------------------------------------------------
# FINAL CLEAN (STRICTLY ML FEATURES)
# --------------------------------------------------
model_features = [
    "close",
    "ret_1d",
    "ret_3d",
    "atr_pct",
    "trend_up",
    "oi_change_pct",
    "regime_LONG_BUILDUP",
    "regime_SHORT_COVERING",
    "regime_NO_DATA",
]

df = df.dropna(subset=model_features).reset_index(drop=True)

# --------------------------------------------------
# SAVE
# --------------------------------------------------
OUT = BASE_DIR / "data/processed/ml/nifty_ml_features_CANONICAL.parquet"
df.to_parquet(OUT, index=False)

print("✅ CANONICAL ML FEATURES BUILT")
print(f"📊 Rows: {len(df)}")
print(df.tail())
print(f"💾 Saved → {OUT}")
