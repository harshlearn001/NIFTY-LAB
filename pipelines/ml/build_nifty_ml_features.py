#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build ML Feature Dataset for NIFTY (GPU Ready)
=============================================

Equity  : data/continuous/master_equity.parquet
Futures : data/processed/futures_ml/nifty_fut_oi_daily.parquet
Options : data/processed/options_ml/nifty_pcr_daily.parquet

Output  : data/processed/ml/nifty_ml_features.parquet
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
from configs.paths import PROC_DIR, CONT_DIR

# =================================================
# PATHS
# =================================================
EQ_FILE   = CONT_DIR / "master_equity.parquet"
FUT_FILE  = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE  = PROC_DIR / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR   = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE  = OUT_DIR / "nifty_ml_features.parquet"

# =================================================
# FILE CHECKS
# =================================================
print("📥 Checking required files...")

for f in [EQ_FILE, FUT_FILE, PCR_FILE]:
    if not f.exists():
        raise FileNotFoundError(f"❌ Missing file: {f}")
    print(f"✅ Found: {f}")

# =================================================
# LOAD DATA
# =================================================
eq  = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE)
pcr = pd.read_parquet(PCR_FILE)

# =================================================
# STANDARDISE EQUITY
# =================================================
eq = eq.rename(columns={
    "TRADE_DATE": "date",
    "DATE": "date",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
})

eq["date"] = pd.to_datetime(eq["date"])
eq = eq.sort_values("date").reset_index(drop=True)

# =================================================
# EQUITY FEATURES
# =================================================
eq["ret_1d"] = eq["close"].pct_change()
eq["ret_3d"] = eq["close"].pct_change(3)

eq["prev_close"] = eq["close"].shift(1)
eq["tr"] = (eq["close"] - eq["prev_close"]).abs()
eq["atr"] = eq["tr"].rolling(14).mean()
eq["atr_pct"] = eq["atr"] / eq["close"]

eq["range_pct"] = (eq["high"] - eq["low"]) / eq["close"]

eq["dma50"] = eq["close"].rolling(50).mean()
eq["trend_up"] = (eq["close"] > eq["dma50"]).astype(int)

eq_feat = eq[[
    "date",
    "close",
    "ret_1d",
    "ret_3d",
    "atr_pct",
    "range_pct",
    "trend_up",
]].copy()

# =================================================
# FUTURES FEATURES (OI ANALYTICS)
# =================================================
fut = fut.rename(columns={
    "price_pct": "price_pct",
    "oi_pct": "oi_pct",
})

fut["date"] = pd.to_datetime(fut["date"])

required_fut = {"date", "price_pct", "oi_pct", "regime"}
missing_fut = required_fut - set(fut.columns)
if missing_fut:
    raise ValueError(f"❌ Missing futures columns: {missing_fut}")

fut_feat = fut[[
    "date",
    "price_pct",
    "oi_pct",
    "regime",
]].copy()

# One-hot encode regimes
regime_dum = pd.get_dummies(fut_feat["regime"], prefix="regime")
fut_feat = pd.concat(
    [fut_feat.drop(columns="regime"), regime_dum],
    axis=1
)

# =================================================
# PCR FEATURES
# =================================================
pcr["date"] = pd.to_datetime(pcr["date"])
pcr_feat = pcr[["date", "pcr"]].sort_values("date")
pcr_feat["pcr_change"] = pcr_feat["pcr"].pct_change()

# =================================================
# MERGE ALL FEATURES
# =================================================
df = eq_feat.merge(fut_feat, on="date", how="inner")
df = df.merge(pcr_feat, on="date", how="left")

# =================================================
# TARGET VARIABLE (ML)
# =================================================
df["next_close"] = df["close"].shift(-1)
df["next_ret"] = (df["next_close"] - df["close"]) / df["close"]

# Binary target: profitable next day
df["target"] = (df["next_ret"] > 0).astype(int)

# =================================================
# FINAL CLEAN
# =================================================
df = df.dropna().reset_index(drop=True)

# =================================================
# SAVE
# =================================================
df.to_parquet(OUT_FILE, index=False)

# =================================================
# SUMMARY
# =================================================
print("\n✅ ML FEATURE DATASET BUILT SUCCESSFULLY")
print(f"📦 File   : {OUT_FILE}")
print(f"📊 Rows   : {len(df)}")
print(f"📊 Columns: {len(df.columns)}")
print("\nSample:")
print(df.head(3))
