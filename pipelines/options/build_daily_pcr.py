#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY PCR BUILDER (PROD SAFE)

✔ Uses master_options.parquet
✔ OPTIDX only
✔ Latest trade date
✔ Smart expiry selection
✔ Robust string normalization
✔ Parquet + CSV output
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

MASTER_OPT = BASE / "data" / "continuous" / "master_options.parquet"
OUT_DIR    = BASE / "data" / "processed" / "options_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_pcr_daily.parquet"
OUT_CSV = OUT_DIR / "nifty_pcr_daily.csv"

print("NIFTY-LAB | DAILY PCR BUILDER")
print("-" * 60)

# --------------------------------------------------
# LOAD
# --------------------------------------------------
df = pd.read_parquet(MASTER_OPT)

# --------------------------------------------------
# NORMALIZE (CRITICAL)
# --------------------------------------------------
df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"], errors="coerce")

df["INSTRUMENT"] = (
    df["INSTRUMENT"]
    .astype(str)
    .str.strip()
    .str.upper()
)

df["OPT_TYPE"] = (
    df["OPT_TYPE"]
    .astype(str)
    .str.strip()      # 🔴 THIS FIXES YOUR ISSUE
    .str.upper()
)

# Normalize OI column
if "OPEN_INT" not in df.columns and "OI" in df.columns:
    df = df.rename(columns={"OI": "OPEN_INT"})

df["OPEN_INT"] = pd.to_numeric(df["OPEN_INT"], errors="coerce").fillna(0)

# --------------------------------------------------
# FILTER: OPTIDX
# --------------------------------------------------
df = df[df["INSTRUMENT"] == "OPTIDX"]

if df.empty:
    print("❌ No OPTIDX rows found")
    exit(0)

# --------------------------------------------------
# LATEST TRADE DATE
# --------------------------------------------------
latest_date = df["TRADE_DATE"].max()
df = df[df["TRADE_DATE"] == latest_date]

# --------------------------------------------------
# SELECT FIRST EXPIRY WITH NON-ZERO OI
# --------------------------------------------------
exp_oi = (
    df.groupby("EXP_DATE")["OPEN_INT"]
      .sum()
      .reset_index()
)

valid = exp_oi[exp_oi["OPEN_INT"] > 0]

if valid.empty:
    print("⚠️ No expiry with non-zero OI found")

    out = pd.DataFrame([{
        "date": latest_date,
        "expiry": None,
        "put_oi": 0.0,
        "call_oi": 0.0,
        "pcr": None,
    }])
else:
    front_expiry = valid["EXP_DATE"].min()
    df = df[df["EXP_DATE"] == front_expiry]

    call_oi = df.loc[df["OPT_TYPE"] == "CE", "OPEN_INT"].sum()
    put_oi  = df.loc[df["OPT_TYPE"] == "PE", "OPEN_INT"].sum()

    pcr = round(float(put_oi / call_oi), 4) if call_oi > 0 else None

    out = pd.DataFrame([{
        "date": latest_date,
        "expiry": front_expiry,
        "put_oi": float(put_oi),
        "call_oi": float(call_oi),
        "pcr": pcr,
    }])

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out.to_parquet(OUT_PQ, index=False)
out.to_csv(OUT_CSV, index=False)

print("✅ PCR BUILD COMPLETE")
print(out)
print(f"💾 Saved : {OUT_PQ}")
print(f"💾 Saved : {OUT_CSV}")
