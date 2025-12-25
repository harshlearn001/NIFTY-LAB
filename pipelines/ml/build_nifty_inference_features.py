#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BUILD NIFTY INFERENCE FEATURES (PROD SAFE)

✔ BASE_DIR safe
✔ Schema-agnostic dates
✔ Safe OI detection
✔ PCR fallback
✔ Inference only (NO training)
"""

import sys
import pandas as pd
from configs.paths import BASE_DIR

# --------------------------------------------------
# PATHS (SINGLE SOURCE OF TRUTH)
# --------------------------------------------------
EQ_FILE  = BASE_DIR / "data/continuous/master_equity.parquet"
FUT_FILE = BASE_DIR / "data/processed/futures_ml/nifty_fut_oi_daily.parquet"
PCR_FILE = BASE_DIR / "data/processed/options_ml/nifty_pcr_daily.parquet"

OUT_DIR  = BASE_DIR / "data/processed/ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_inference_features.parquet"

print("Loading data...")

# --------------------------------------------------
# LOAD (SAFE)
# --------------------------------------------------
if not EQ_FILE.exists():
    print("❌ master_equity.parquet missing")
    sys.exit(0)

eq = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE) if FUT_FILE.exists() else None
pcr = pd.read_parquet(PCR_FILE) if PCR_FILE.exists() else None

# --------------------------------------------------
# DATE NORMALIZATION
# --------------------------------------------------
def normalize_date(df):
    for col in ["date", "DATE", "TRADE_DATE", "TradeDate"]:
        if col in df.columns:
            df["date"] = pd.to_datetime(df[col])
            return df
    raise KeyError(f"No date column in {df.columns.tolist()}")

eq = normalize_date(eq)
if fut is not None:
    fut = normalize_date(fut)
if pcr is not None:
    pcr = normalize_date(pcr)

# --------------------------------------------------
# ALIGN LATEST DATE
# --------------------------------------------------
eq = eq.sort_values("date")
eq_latest = eq.tail(2)
latest_date = eq_latest["date"].iloc[-1]

if fut is not None and not fut.empty:
    fut = fut.sort_values("date").tail(1)

if pcr is not None and not pcr.empty:
    pcr = pcr.sort_values("date").tail(1)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def detect_oi_column(df):
    for col in ["oi", "OI", "OPEN_INT", "OPENINTEREST"]:
        if col in df.columns:
            return col
    return None

# --------------------------------------------------
# FEATURE BUILD
# --------------------------------------------------
features = pd.DataFrame({"date": [latest_date]})

# --- Equity
features["close"] = eq_latest["CLOSE"].iloc[-1]

if len(eq_latest) == 2:
    prev = eq_latest["CLOSE"].iloc[0]
    curr = eq_latest["CLOSE"].iloc[1]
    features["ret_1d"] = (curr / prev) - 1.0
else:
    features["ret_1d"] = 0.0

# --- Futures OI
if fut is not None and not fut.empty:
    oi_col = detect_oi_column(fut)
    features["oi"] = fut[oi_col].iloc[0] if oi_col else 0.0
    features["oi_change"] = fut["oi_change"].iloc[0] if "oi_change" in fut.columns else 0.0
else:
    features["oi"] = 0.0
    features["oi_change"] = 0.0

# --- PCR
if pcr is not None and not pcr.empty and "pcr" in pcr.columns:
    features["pcr"] = float(pcr["pcr"].iloc[0])
else:
    features["pcr"] = 1.0  # neutral

# --------------------------------------------------
# SAVE
# --------------------------------------------------
features.to_parquet(OUT_FILE, index=False)

print("✅ INFERENCE FEATURES BUILT")
print(features)
print(f"💾 Saved → {OUT_FILE}")
