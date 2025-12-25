#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ML FEATURES (EQUITY + FUTURES OI)

✔ Long history (2016 → present)
✔ NO PCR (options start late)
✔ NO volume filtering (Yahoo index limitation)
✔ No leakage
✔ GPU ready
"""

import sys
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PROJECT ROOT
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import CONT_DIR, PROC_DIR

# --------------------------------------------------
# PATHS
# --------------------------------------------------
EQ_FILE  = CONT_DIR / "master_equity.parquet"
FUT_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_historical.parquet"


OUT_DIR = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_ml_features_hist_no_pcr.parquet"
OUT_CSV = OUT_DIR / "nifty_ml_features_hist_no_pcr.csv"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
print("📥 Loading Equity + Futures datasets...")

eq  = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE)

# --------------------------------------------------
# STANDARDISE EQUITY (HISTORICAL SAFE)
# --------------------------------------------------
eq = eq.rename(columns={
    "DATE": "date",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
})

eq["date"] = pd.to_datetime(eq["date"])
eq = eq.sort_values("date").reset_index(drop=True)

# --------------------------------------------------
# EQUITY FEATURES
# --------------------------------------------------
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

# --------------------------------------------------
# FUTURES OI FEATURES
# --------------------------------------------------
fut = fut.rename(columns={
    "TRADE_DATE": "date",
    "price_pct_change": "price_pct",
    "oi_pct_change": "oi_pct",
    "oi_signal": "regime",
})

fut["date"] = pd.to_datetime(fut["date"])

required = {"date", "price_pct", "oi_pct", "regime"}
missing = required - set(fut.columns)
if missing:
    raise RuntimeError(f"❌ Missing futures columns: {missing}")

fut_feat = fut[[
    "date",
    "price_pct",
    "oi_pct",
    "regime",
]].copy()

# One-hot encode OI regimes
regime_dum = pd.get_dummies(fut_feat["regime"], prefix="regime")
fut_feat = pd.concat(
    [fut_feat.drop(columns="regime"), regime_dum],
    axis=1
)

# --------------------------------------------------
# MERGE (STRICT DATE ALIGNMENT)
# --------------------------------------------------
df = eq_feat.merge(fut_feat, on="date", how="inner")

# --------------------------------------------------
# TARGET (NEXT DAY)
# --------------------------------------------------
df["next_close"] = df["close"].shift(-1)
df["next_ret"] = (df["next_close"] - df["close"]) / df["close"]
df["target"] = (df["next_ret"] > 0).astype(int)

# --------------------------------------------------
# FINAL CLEAN
# --------------------------------------------------
df = df.dropna().reset_index(drop=True)

# --------------------------------------------------
# SAVE
# --------------------------------------------------
df.to_parquet(OUT_PQ, index=False)
df.to_csv(OUT_CSV, index=False)

# --------------------------------------------------
# SUMMARY
# --------------------------------------------------
print("\n✅ HISTORICAL ML DATASET (NO PCR) READY")
print(f"📦 Parquet : {OUT_PQ}")
print(f"📦 CSV     : {OUT_CSV}")
print(f"📊 Rows    : {len(df):,}")
print(f"📊 Columns : {len(df.columns)}")
print(f"📅 From    : {df['date'].min().date()}")
print(f"📅 To      : {df['date'].max().date()}")
print("\nSample:")
print(df.head(3))
