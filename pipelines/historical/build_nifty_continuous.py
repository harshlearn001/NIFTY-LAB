#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BUILD NIFTY CONTINUOUS SERIES

✔ Uses historical NIFTY equity data
✔ Clean OHLC
✔ Sorted, deduplicated
✔ Production-safe
"""

import pandas as pd
from configs.paths import RAW_HIST_DIR, CONT_DIR

# --------------------------------------------------
# INPUT / OUTPUT
# --------------------------------------------------
SRC = RAW_HIST_DIR / "equity" / "nifty50_equ_hist_2007.csv"
OUT = CONT_DIR / "nifty_continuous.parquet"

# --------------------------------------------------
# LOAD
# --------------------------------------------------
df = pd.read_csv(SRC)

# --------------------------------------------------
# NORMALIZE COLUMNS
# --------------------------------------------------
df.columns = df.columns.str.strip().str.upper()

# Detect date column
for c in ["DATE", "TIMESTAMP", "TRADE_DATE"]:
    if c in df.columns:
        df = df.rename(columns={c: "DATE"})
        break

# --------------------------------------------------
# DATE CLEAN
# --------------------------------------------------
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df = df.dropna(subset=["DATE"])

# --------------------------------------------------
# REQUIRED OHLC
# --------------------------------------------------
df = df[["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]]

# --------------------------------------------------
# SORT & DEDUP
# --------------------------------------------------
df = (
    df.drop_duplicates(subset=["DATE"])
      .sort_values("DATE")
      .reset_index(drop=True)
)

# --------------------------------------------------
# SAVE
# --------------------------------------------------
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_parquet(OUT, index=False)

print("✅ NIFTY CONTINUOUS SERIES CREATED")
print(df.tail())
