#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-14 | BUILD NIFTY FUTURES OI HISTORICAL (AUTO-DETECT)

✔ NSE column-name safe
✔ Detects DATE & OPEN INTEREST automatically
✔ Computes OI_CHANGE_PCT
✔ ML & backtest ready
"""

from pathlib import Path
import pandas as pd

# ======================================================
# PATHS
# ======================================================
BASE_DIR = Path(r"H:\NIFTY-LAB-Trial")

IN_FILE = BASE_DIR / "data" / "processed" / "futures_ml" / "nifty_fut_oi_daily.csv"
OUT_DIR = BASE_DIR / "data" / "processed" / "futures_ml"
OUT_FILE = OUT_DIR / "nifty_fut_oi_historical.parquet"

# ======================================================
# LOAD
# ======================================================
if not IN_FILE.exists():
    raise FileNotFoundError(f"❌ Missing input file: {IN_FILE}")

df = pd.read_csv(IN_FILE)
df.columns = df.columns.str.strip().str.upper()

# ======================================================
# AUTO-DETECT DATE COLUMN
# ======================================================
DATE_CANDIDATES = [
    "DATE", "TRAD_DT", "TRADE_DATE", "BUSINESS_DATE"
]

date_col = next((c for c in DATE_CANDIDATES if c in df.columns), None)
if not date_col:
    raise RuntimeError(f"❌ DATE column not found. Columns: {df.columns.tolist()}")

df["DATE"] = pd.to_datetime(df[date_col])

# ======================================================
# AUTO-DETECT OPEN INTEREST COLUMN
# ======================================================
OI_CANDIDATES = [
    "OPEN_INT", "OPENINTEREST", "OPEN_INTEREST", "OI"
]

oi_col = next((c for c in OI_CANDIDATES if c in df.columns), None)
if not oi_col:
    raise RuntimeError(f"❌ OPEN INTEREST column not found. Columns: {df.columns.tolist()}")

df["OPEN_INT"] = pd.to_numeric(df[oi_col], errors="coerce").fillna(0)

# ======================================================
# SORT
# ======================================================
df = df.sort_values("DATE").reset_index(drop=True)

# ======================================================
# OI CHANGE %
# ======================================================
df["OI_CHANGE_PCT"] = df["OPEN_INT"].pct_change().fillna(0.0)

# ======================================================
# SAVE
# ======================================================
df_out = df[["DATE", "OPEN_INT", "OI_CHANGE_PCT"]]
df_out.to_parquet(OUT_FILE, index=False)

print("\n✅ NIFTY FUTURES OI HISTORICAL BUILT")
print(df_out.tail())
print(f"\n📁 Saved → {OUT_FILE}")
