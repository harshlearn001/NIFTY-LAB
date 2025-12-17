#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BUILD NIFTY INFERENCE FEATURES (PROD SAFE)

✔ Schema-agnostic (DATE / TRADE_DATE)
✔ Handles missing PCR gracefully
✔ Futures OI + PCR + Equity
✔ Inference-only (NO training)
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

EQ_FILE  = BASE / "data" / "continuous" / "master_equity.parquet"
FUT_FILE = BASE / "data" / "processed" / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE = BASE / "data" / "processed" / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR  = BASE / "data" / "processed" / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_inference_features.parquet"

print("📥 Loading data...")

# --------------------------------------------------
# LOAD (SAFE)
# --------------------------------------------------
if not EQ_FILE.exists():
    print("❌ master_equity missing")
    exit(0)

eq = pd.read_parquet(EQ_FILE)

fut = pd.read_parquet(FUT_FILE) if FUT_FILE.exists() else None
pcr = pd.read_parquet(PCR_FILE) if PCR_FILE.exists() else None

# --------------------------------------------------
# DATE NORMALIZATION (CRITICAL FIX)
# --------------------------------------------------
def normalize_date(df):
    if "TRADE_DATE" in df.columns:
        df["date"] = pd.to_datetime(df["TRADE_DATE"])
    elif "DATE" in df.columns:
        df["date"] = pd.to_datetime(df["DATE"])
    elif "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    else:
        raise KeyError("No date column found")

    return df


eq = normalize_date(eq)

if fut is not None:
    fut = normalize_date(fut)

if pcr is not None:
    pcr = normalize_date(pcr)

# --------------------------------------------------
# LATEST DATE ALIGNMENT
# --------------------------------------------------
latest_date = eq["date"].max()
eq = eq[eq["date"] == latest_date]

if fut is not None:
    fut = fut[fut["date"] == fut["date"].max()]

if pcr is not None:
    pcr = pcr[pcr["date"] == pcr["date"].max()]

# --------------------------------------------------
# FEATURE BUILD
# --------------------------------------------------
features = pd.DataFrame({"date": [latest_date]})

# --- Equity
features["close"] = eq["CLOSE"].values[0]
features["ret_1d"] = eq["CLOSE"].pct_change().fillna(0).values[0]

# --- Futures OI
if fut is not None and not fut.empty:
    features["oi"] = fut["OI"].values[0]
    features["oi_change"] = fut["oi_change"].values[0]
else:
    features["oi"] = 0.0
    features["oi_change"] = 0.0

# --- PCR (SAFE)
if pcr is not None and not pcr.empty and pd.notna(pcr["pcr"].values[0]):
    features["pcr"] = pcr["pcr"].values[0]
else:
    features["pcr"] = 1.0  # neutral fallback

# --------------------------------------------------
# SAVE
# --------------------------------------------------
features.to_parquet(OUT_FILE, index=False)

print("✅ INFERENCE FEATURES BUILT")
print(features)
print(f"💾 Saved : {OUT_FILE}")
