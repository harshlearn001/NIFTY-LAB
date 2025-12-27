#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY INFERENCE FEATURES (SAFE)

✔ Uses latest master equity + futures
✔ No leakage
✔ Matches training schema
✔ Subprocess safe
"""

import sys
from pathlib import Path
import pandas as pd

# ==================================================
# PROJECT ROOT (CRITICAL FIX)
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import (
    CONT_DIR,
    PROC_DIR,
)

# ==================================================
# PATHS
# ==================================================
EQ_FILE  = CONT_DIR / "master_equity.parquet"
FUT_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_historical.parquet"

OUT_DIR  = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ   = OUT_DIR / "nifty_inference_features.parquet"
OUT_CSV  = OUT_DIR / "nifty_inference_features.csv"

# ==================================================
# LOAD DATA
# ==================================================
print("📥 Loading master equity + futures...")

eq  = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE)

# ==================================================
# STANDARDISE EQUITY
# ==================================================
eq = eq.rename(columns={
    "DATE": "date",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
})

eq["date"] = pd.to_datetime(eq["date"])
eq = eq.sort_values("date").reset_index(drop=True)

# ==================================================
# EQUITY FEATURES (MATCH TRAINING)
# ==================================================
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

# ==================================================
# FUTURES OI FEATURES
# ==================================================
fut = fut.rename(columns={
    "TRADE_DATE": "date",
    "price_pct_change": "price_pct",
    "oi_pct_change": "oi_pct",
    "oi_signal": "regime",
})

fut["date"] = pd.to_datetime(fut["date"])

fut_feat = fut[[
    "date",
    "oi_pct",
    "regime",
]].copy()

regime_dum = pd.get_dummies(fut_feat["regime"], prefix="regime")
fut_feat = pd.concat(
    [fut_feat.drop(columns="regime"), regime_dum],
    axis=1
)

# ==================================================
# MERGE (LATEST ONLY)
# ==================================================
df = eq_feat.merge(fut_feat, on="date", how="inner")

# keep latest row only (daily inference)
df = df.tail(1).reset_index(drop=True)

# ==================================================
# SAVE
# ==================================================
df.to_parquet(OUT_PQ, index=False)
df.to_csv(OUT_CSV, index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n✅ DAILY INFERENCE FEATURES READY")
print(f"📦 Parquet : {OUT_PQ}")
print(f"📦 CSV     : {OUT_CSV}")
print("\nRow:")
print(df.T)
