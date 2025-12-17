#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | ML INFERENCE FEATURE BUILDER (EOD SAFE)
==================================================

✔ Equity, Futures, Options must be SAME EOD date
✔ Removes intraday equity candles (VOLUME == 0)
✔ Produces ONE ROW for prediction
✔ NO target, NO next_close leakage

Output:
  data/processed/ml/nifty_ml_inference.parquet
  data/processed/ml/nifty_ml_inference.csv
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
from configs.paths import PROC_DIR, CONT_DIR

# =================================================
# PATHS
# =================================================
EQ_FILE   = CONT_DIR / "master_equity.parquet"
FUT_FILE  = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE  = PROC_DIR / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_ml_inference.parquet"
OUT_CSV = OUT_DIR / "nifty_ml_inference.csv"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading data...")

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

# 🚨 CRITICAL FIX: remove intraday / partial candles
eq = eq[eq["VOLUME"] > 0].copy()

eq = eq.sort_values("date").reset_index(drop=True)

# =================================================
# DATE ALIGNMENT GUARD
# =================================================
eq_max  = eq["date"].max()
fut_max = pd.to_datetime(fut["TRADE_DATE"]).max()
pcr_max = pd.to_datetime(pcr["date"]).max()

print("\n📅 DATA AVAILABILITY CHECK")
print(f"Equity  : {eq_max.date()}")
print(f"Futures : {fut_max.date()}")
print(f"Options : {pcr_max.date()}")

if not (eq_max == fut_max == pcr_max):
    print("\n⛔ INFERENCE SKIPPED — DATA MISALIGNMENT")
    sys.exit(0)

# =================================================
# EQUITY FEATURES (ROLLING SAFE)
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
]]

# =================================================
# FUTURES FEATURES
# =================================================
fut = fut.rename(columns={
    "TRADE_DATE": "date",
    "price_pct_change": "price_pct",
    "oi_pct_change": "oi_pct",
    "oi_signal": "regime",
})

fut["date"] = pd.to_datetime(fut["date"])

fut_feat = fut[[
    "date",
    "price_pct",
    "oi_pct",
    "regime",
]]

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
# MERGE FEATURES
# =================================================
df = eq_feat.merge(fut_feat, on="date", how="inner")
df = df.merge(pcr_feat, on="date", how="left")

# =================================================
# KEEP ONLY LATEST ROW (INFERENCE)
# =================================================
df = df.sort_values("date").tail(1).reset_index(drop=True)

# =================================================
# SAVE
# =================================================
df.to_parquet(OUT_PQ, index=False)
df.to_csv(OUT_CSV, index=False)

# =================================================
# SUMMARY
# =================================================
print("\n✅ ML INFERENCE FEATURES READY")
print(f"📅 Date : {df.loc[0, 'date'].date()}")
print(f"📦 Rows : {len(df)}")
print(f"📦 File : {OUT_PQ}")
print("\nFeatures:")
print(df.T)
