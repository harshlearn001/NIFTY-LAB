#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | OPTIONS PCR HISTORICAL ANALYTICS

✔ Builds daily index-wide PCR (Put/Call Ratio)
✔ Cleans OPT_TYPE properly (CE / PE)
✔ Uses Open Interest
✔ Full historical coverage
"""

from pathlib import Path
import pandas as pd
import numpy as np

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

OPT_MASTER = BASE / "data" / "continuous" / "master_options.parquet"

OUT_DIR = BASE / "data" / "processed" / "options_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_pcr_historical.parquet"
OUT_CSV = OUT_DIR / "nifty_pcr_historical.csv"

# --------------------------------------------------
# LOAD
# --------------------------------------------------
print("📥 Loading options master...")
df = pd.read_parquet(OPT_MASTER)

if df.empty:
    raise RuntimeError("❌ Options master is empty")

# --------------------------------------------------
# STANDARDISE
# --------------------------------------------------
df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
df["OPEN_INT"]   = pd.to_numeric(df["OPEN_INT"], errors="coerce")

# 🔑 CRITICAL FIX
df["OPT_TYPE"] = (
    df["OPT_TYPE"]
    .astype(str)
    .str.strip()
    .str.upper()
)

df = df[df["OPT_TYPE"].isin(["CE", "PE"])]
df = df.dropna(subset=["TRADE_DATE", "OPEN_INT"])

# --------------------------------------------------
# AGGREGATE DAILY OI
# --------------------------------------------------
daily = (
    df.groupby(["TRADE_DATE", "OPT_TYPE"])["OPEN_INT"]
      .sum()
      .unstack(fill_value=0)
      .reset_index()
)

# Ensure both columns exist
for col in ["CE", "PE"]:
    if col not in daily.columns:
        daily[col] = 0.0

# --------------------------------------------------
# PCR CALCULATION
# --------------------------------------------------
daily["pcr"] = np.where(
    daily["CE"] > 0,
    daily["PE"] / daily["CE"],
    np.nan
)

daily = daily.sort_values("TRADE_DATE")
daily["pcr_change"] = daily["pcr"].pct_change()

out = daily[["TRADE_DATE", "pcr", "pcr_change"]].dropna()

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out.to_parquet(OUT_PQ, index=False)
out.to_csv(OUT_CSV, index=False)

print("✅ OPTIONS PCR HISTORY READY")
print(f"📊 Rows : {len(out):,}")
print(f"📅 From : {out.TRADE_DATE.min().date()} → {out.TRADE_DATE.max().date()}")
print(f"💾 File : {OUT_PQ}")
