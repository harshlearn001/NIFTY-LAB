#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BUILD NIFTY INFERENCE FEATURES (PROD SAFE)

✔ Schema-agnostic dates
✔ Safe OI detection
✔ PCR fallback
✔ Equity + Futures + Options
✔ Inference only (NO training)
"""

from pathlib import Path
import pandas as pd
import sys

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

print("Loading data...")

# --------------------------------------------------
# LOAD (SAFE)
# --------------------------------------------------
if not EQ_FILE.exists():
    print(" master_equity.parquet missing")
    sys.exit(0)

eq = pd.read_parquet(EQ_FILE)

fut = pd.read_parquet(FUT_FILE) if FUT_FILE.exists() else None
pcr = pd.read_parquet(PCR_FILE) if PCR_FILE.exists() else None

# --------------------------------------------------
# DATE NORMALIZATION
# --------------------------------------------------
def normalize_date(df):
    df = df.copy()

    for col in ["TRADE_DATE", "DATE", "date"]:
        if col in df.columns:
            df["date"] = pd.to_datetime(df[col])
            return df

    raise KeyError(f"No date column found in {list(df.columns)}")


eq = normalize_date(eq)

if fut is not None:
    fut = normalize_date(fut)

if pcr is not None:
    pcr = normalize_date(pcr)

# --------------------------------------------------
# LATEST DATE ALIGNMENT
# --------------------------------------------------
latest_date = eq["date"].max()

# equity: keep last 2 rows for return calc
eq_latest = eq.sort_values("date").tail(2)

if fut is not None and not fut.empty:
    fut = fut[fut["date"] == fut["date"].max()]

if pcr is not None and not pcr.empty:
    pcr = pcr[pcr["date"] == pcr["date"].max()]

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

# --- Equity features
features["close"] = eq_latest["CLOSE"].iloc[-1]

if len(eq_latest) == 2:
    prev_close = eq_latest["CLOSE"].iloc[0]
    curr_close = eq_latest["CLOSE"].iloc[1]
    features["ret_1d"] = (curr_close / prev_close) - 1.0
else:
    features["ret_1d"] = 0.0

# --- Futures OI features
if fut is not None and not fut.empty:
    oi_col = detect_oi_column(fut)

    if oi_col:
        features["oi"] = fut[oi_col].values[0]
    else:
        features["oi"] = 0.0

    features["oi_change"] = fut["oi_change"].values[0] if "oi_change" in fut.columns else 0.0
else:
    features["oi"] = 0.0
    features["oi_change"] = 0.0

# --- PCR feature (SAFE)
if pcr is not None and not pcr.empty and "pcr" in pcr.columns and pd.notna(pcr["pcr"].values[0]):
    features["pcr"] = pcr["pcr"].values[0]
else:
    features["pcr"] = 1.0  # neutral fallback

# --------------------------------------------------
# SAVE
# --------------------------------------------------
features.to_parquet(OUT_FILE, index=False)

print("INFERENCE FEATURES BUILT")
print(features)
print(f" Saved : {OUT_FILE}")
