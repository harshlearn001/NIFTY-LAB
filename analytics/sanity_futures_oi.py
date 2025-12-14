#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sanity Checks for NIFTY Futures OI Analytics
--------------------------------------------
Validates:
- File existence
- Required columns
- No duplicate dates
- Sorted by date
- No negative OI
- Valid regimes only
- No future dates
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]  # H:\NIFTY-LAB
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
from configs.paths import PROC_DIR

# =================================================
# PATHS
# =================================================
OI_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading futures OI analytics file...")

if not OI_FILE.exists():
    raise FileNotFoundError(f"❌ Missing file: {OI_FILE}")

df = pd.read_parquet(OI_FILE)

print(f"📊 Rows loaded: {len(df)}")

# =================================================
# REQUIRED COLUMNS
# =================================================
REQUIRED_COLS = {
    "date",
    "expiry",
    "close",
    "open_interest",
    "price_pct",
    "oi_pct",
    "regime",
}

missing = REQUIRED_COLS - set(df.columns)
if missing:
    raise ValueError(f"❌ Missing columns: {missing}")

print("✅ Required columns present")

# =================================================
# DATE VALIDATION
# =================================================
df["date"] = pd.to_datetime(df["date"])
today = pd.Timestamp(datetime.now().date())

if (df["date"] > today).any():
    raise ValueError("❌ Found future trade dates")

print("✅ No future dates")

# =================================================
# DUPLICATE DATE CHECK
# =================================================
dup_dates = df["date"].duplicated().sum()
if dup_dates > 0:
    raise ValueError(f"❌ Duplicate dates found: {dup_dates}")

print("✅ No duplicate dates")

# =================================================
# SORT ORDER CHECK
# =================================================
if not df["date"].is_monotonic_increasing:
    raise ValueError("❌ Data is not sorted by date")

print("✅ Data sorted by date")

# =================================================
# OPEN INTEREST CHECK
# =================================================
if (df["open_interest"] < 0).any():
    raise ValueError("❌ Negative open interest detected")

print("✅ Open interest values valid")

# =================================================
# REGIME VALIDATION
# =================================================
VALID_REGIMES = {
    "LONG_BUILDUP",
    "SHORT_BUILDUP",
    "LONG_UNWINDING",
    "SHORT_COVERING",
    "NEUTRAL",
    "NA",
}

invalid = set(df["regime"].unique()) - VALID_REGIMES
if invalid:
    raise ValueError(f"❌ Invalid regime values: {invalid}")

print("✅ Regime values valid")

# =================================================
# FINAL SUMMARY
# =================================================
print("\n🎯 SANITY CHECK PASSED")
print("📦 File:", OI_FILE)
print("📅 Date range:", df["date"].min().date(), "→", df["date"].max().date())
print("📊 Regime distribution:")
print(df["regime"].value_counts())
