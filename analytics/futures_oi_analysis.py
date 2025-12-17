#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FUTURES OI ANALYTICS (PROD SAFE)

✔ Front-month FUTIDX
✔ Single-day tolerant
✔ Always emits 1 row per date
✔ ML-ready output
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

MASTER_FUT = BASE / "data" / "continuous" / "master_futures.parquet"
OUT_DIR    = BASE / "data" / "processed" / "futures_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_fut_oi_daily.parquet"

print("NIFTY-LAB | FUTURES OI ANALYTICS")
print("-" * 60)

# --------------------------------------------------
# LOAD
# --------------------------------------------------
if not MASTER_FUT.exists():
    print("❌ master_futures.parquet not found")
    exit(0)

df = pd.read_parquet(MASTER_FUT)

if df.empty:
    print("❌ master_futures empty")
    exit(0)

# --------------------------------------------------
# NORMALIZE
# --------------------------------------------------
df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"])

# --------------------------------------------------
# FRONT-MONTH SELECTION
# --------------------------------------------------
df = (
    df.sort_values(["TRADE_DATE", "EXP_DATE"])
      .groupby("TRADE_DATE", as_index=False)
      .first()
)

# --------------------------------------------------
# OI CHANGE (SAFE)
# --------------------------------------------------
df = df.sort_values("TRADE_DATE")
df["oi_change"] = df["OI"].diff()

# --------------------------------------------------
# REGIME LOGIC (SINGLE-DAY SAFE)
# --------------------------------------------------
def classify(row):
    if pd.isna(row["oi_change"]):
        return "NO_DATA"
    if row["oi_change"] > 0:
        return "LONG_BUILDUP"
    if row["oi_change"] < 0:
        return "SHORT_COVERING"
    return "NEUTRAL"

df["oi_signal"] = df.apply(classify, axis=1)

# --------------------------------------------------
# FINAL OUTPUT
# --------------------------------------------------
out = df[["TRADE_DATE", "OI", "oi_change", "oi_signal"]].rename(
    columns={"TRADE_DATE": "date"}
)

out.to_parquet(OUT_FILE, index=False)

print("✅ FUTURES OI ANALYTICS COMPLETE")
print(out.tail(3))
print(f"💾 Saved : {OUT_FILE}")
