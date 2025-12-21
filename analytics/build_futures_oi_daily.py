#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BUILD DAILY FUTURES OI (ML READY)

✔ Uses master_futures.parquet
✔ One row per trading day
✔ Adds OI change
✔ Adds OI regime
✔ Writes futures_ml file
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")

MASTER_FUT = BASE / "data" / "continuous" / "master_futures.parquet"
OUT_DIR    = BASE / "data" / "processed" / "futures_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_fut_oi_daily.parquet"

print("Building daily futures OI ML file...")

df = pd.read_parquet(MASTER_FUT)

# --------------------------------------------------
# NORMALIZE
# --------------------------------------------------
df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"])

# Use nearest expiry per day
df = (
    df.sort_values(["TRADE_DATE", "EXP_DATE"])
      .groupby("TRADE_DATE")
      .first()
      .reset_index()
)

# --------------------------------------------------
# OI METRICS
# --------------------------------------------------
df["oi"] = pd.to_numeric(df["OPEN_INTEREST"], errors="coerce")
df["oi_change"] = df["oi"].diff()

# --------------------------------------------------
# OI REGIME
# --------------------------------------------------
def classify_oi(row):
    if pd.isna(row["oi_change"]):
        return "NO_DATA"
    if row["oi_change"] > 0:
        return "LONG_BUILDUP"
    if row["oi_change"] < 0:
        return "LONG_UNWINDING"
    return "NO_CHANGE"

df["regime"] = df.apply(classify_oi, axis=1)

# --------------------------------------------------
# FINAL OUTPUT
# --------------------------------------------------
out = df[["TRADE_DATE", "oi", "oi_change", "regime"]].rename(
    columns={"TRADE_DATE": "date"}
)

out.to_parquet(OUT_FILE, index=False)
out.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

print(" Daily futures OI file built")
print(out.tail())
print(f" Saved : {OUT_FILE}")
