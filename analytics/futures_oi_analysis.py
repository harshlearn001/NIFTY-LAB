#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY Futures OI + Price Action Analytics
----------------------------------------
Builds daily futures regimes:
- LONG_BUILDUP
- SHORT_BUILDUP
- LONG_UNWINDING
- SHORT_COVERING

Input:
- data/continuous/master_futures.parquet

Output:
- data/processed/futures_ml/nifty_fut_oi_daily.parquet
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # H:\NIFTY-LAB
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
from configs.paths import CONT_DIR, PROC_DIR

# =================================================
# PATHS
# =================================================
FUT_MASTER = CONT_DIR / "master_futures.parquet"

OUT_DIR = PROC_DIR / "futures_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_fut_oi_daily.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading futures master...")

if not FUT_MASTER.exists():
    raise FileNotFoundError(f"❌ File not found: {FUT_MASTER}")

df = pd.read_parquet(FUT_MASTER)

# =================================================
# STANDARDISE COLUMN NAMES
# =================================================
df = df.rename(columns={
    "SYMBOL": "symbol",
    "TRADE_DATE": "date",
    "EXP_DATE": "expiry",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CLOSE": "close",
    "OI": "open_interest",
})

# =================================================
# FILTER: NIFTY ONLY
# =================================================
df = df[df["symbol"] == "NIFTY"].copy()

df["date"] = pd.to_datetime(df["date"])
df["expiry"] = pd.to_datetime(df["expiry"])

# =================================================
# PICK CURRENT (NEAREST) EXPIRY PER DAY
# =================================================
df = df.sort_values(["date", "expiry"])
df["rank"] = df.groupby("date")["expiry"].rank(method="first")
df = df[df["rank"] == 1].drop(columns="rank")

# =================================================
# SORT BY DATE
# =================================================
df = df.sort_values("date")

# =================================================
# DAILY % CHANGES
# =================================================
df["price_pct"] = df["close"].pct_change() * 100
df["oi_pct"] = df["open_interest"].pct_change() * 100

# =================================================
# REGIME CLASSIFICATION
# =================================================
def classify(row):
    if pd.isna(row.price_pct) or pd.isna(row.oi_pct):
        return "NA"

    if row.price_pct > 0 and row.oi_pct > 0:
        return "LONG_BUILDUP"
    if row.price_pct < 0 and row.oi_pct > 0:
        return "SHORT_BUILDUP"
    if row.price_pct < 0 and row.oi_pct < 0:
        return "LONG_UNWINDING"
    if row.price_pct > 0 and row.oi_pct < 0:
        return "SHORT_COVERING"

    return "NEUTRAL"


df["regime"] = df.apply(classify, axis=1)

# =================================================
# FINAL OUTPUT
# =================================================
out_cols = [
    "date",
    "expiry",
    "close",
    "open_interest",
    "price_pct",
    "oi_pct",
    "regime",
]

df_out = df[out_cols].copy()
# Save Parquet (ML / fast)
df_out.to_parquet(OUT_FILE, index=False)

# Save CSV (human readable)
OUT_FILE_CSV = OUT_FILE.with_suffix(".csv")
df_out.to_csv(OUT_FILE_CSV, index=False)

print("✅ Futures OI analytics built successfully")
print(f"📦 Parquet saved to: {OUT_FILE}")
print(f"📄 CSV saved to: {OUT_FILE_CSV}")
print(f"📊 Rows: {len(df_out)}")
