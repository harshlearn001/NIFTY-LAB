#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ML FEATURE BUILDER (TRAINING)
==================================================
✔ Long history
✔ Equity + Futures OI only
✔ NO PCR (important!)
✔ GPU-ready
"""

from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(r"H:\NIFTY-LAB")

EQ_FILE  = BASE / "data/continuous/master_equity.parquet"
FUT_FILE = BASE / "data/processed/futures_ml/nifty_fut_oi_daily.parquet"

OUT_DIR = BASE / "data/processed/ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ = OUT_DIR / "nifty_ml_features_train.parquet"

print("📥 Loading datasets...")

eq  = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE)

# -----------------------------
# STANDARDIZE
# -----------------------------
eq = eq.rename(columns={
    "DATE": "date",
    "CLOSE": "close",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
})
eq["date"] = pd.to_datetime(eq["date"])

fut = fut.rename(columns={
    "TRADE_DATE": "date",
    "oi_signal": "regime",
})
fut["date"] = pd.to_datetime(fut["date"])

# -----------------------------
# EQUITY FEATURES
# -----------------------------
eq = eq.sort_values("date")

eq["ret_1d"] = eq["close"].pct_change()
eq["ret_3d"] = eq["close"].pct_change(3)

eq["atr"] = (eq["high"] - eq["low"]).rolling(14).mean()
eq["atr_pct"] = eq["atr"] / eq["close"]

eq["dma50"] = eq["close"].rolling(50).mean()
eq["trend_up"] = (eq["close"] > eq["dma50"]).astype(int)

eq_feat = eq[
    ["date", "close", "ret_1d", "ret_3d", "atr_pct", "trend_up"]
]

# -----------------------------
# FUTURES FEATURES
# -----------------------------
fut_feat = fut[["date", "oi_change", "regime"]].copy()
fut_feat["oi_change_pct"] = (
    fut_feat["oi_change"] /
    fut_feat["oi_change"].abs().rolling(20).mean()
)

regime_dum = pd.get_dummies(fut_feat["regime"], prefix="regime")

fut_feat = pd.concat(
    [fut_feat[["date", "oi_change_pct"]], regime_dum],
    axis=1
)

# -----------------------------
# MERGE (EQUITY + FUTURES)
# -----------------------------
print("🔗 Aligning Equity + Futures...")

df = eq_feat.merge(fut_feat, on="date", how="inner")

# -----------------------------
# TARGET
# -----------------------------
df["next_close"] = df["close"].shift(-1)
df["next_ret"] = (df["next_close"] - df["close"]) / df["close"]
df["target"] = (df["next_ret"] > 0).astype(int)

df = df.dropna().reset_index(drop=True)

# -----------------------------
# SAVE
# -----------------------------
df.to_parquet(OUT_PQ, index=False)

print("\n✅ TRAINING DATASET READY")
print(f"📦 File  : {OUT_PQ}")
print(f"📊 Rows  : {len(df)}")
print(f"📊 Cols  : {len(df.columns)}")
