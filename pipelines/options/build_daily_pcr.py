#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY PCR BUILDER (PROD SAFE)

✔ Uses master_options.parquet
✔ OPTIDX only (NIFTY universe)
✔ Latest trade date auto-detected
✔ Front expiry only
✔ OI-based PCR
✔ ML-ready output
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

OUT_FILE = OUT_DIR / "nifty_pcr_daily.parquet"

print("NIFTY-LAB | DAILY PCR BUILDER")
print("-" * 60)

# --------------------------------------------------
# LOAD
# --------------------------------------------------
if not MASTER_OPT.exists():
    print("❌ master_options.parquet not found")
    exit(0)

df = pd.read_parquet(MASTER_OPT)

if df.empty:
    print("❌ master_options empty")
    exit(0)

# --------------------------------------------------
# NORMALIZE
# --------------------------------------------------
df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"], errors="coerce")

df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.upper()
df["OPT_TYPE"]   = df["OPT_TYPE"].astype(str).str.upper()

# --------------------------------------------------
# FILTER: OPTIDX ONLY (NIFTY OPTIONS)
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
# FRONT EXPIRY ONLY
# --------------------------------------------------
front_expiry = df["EXP_DATE"].min()
df = df[df["EXP_DATE"] == front_expiry]

# --------------------------------------------------
# PCR CALCULATION (OI BASED)
# --------------------------------------------------
call_oi = df.loc[df["OPT_TYPE"] == "CE", "OPEN_INT"].sum()
put_oi  = df.loc[df["OPT_TYPE"] == "PE", "OPEN_INT"].sum()

if call_oi <= 0:
    pcr = None
else:
    pcr = round(float(put_oi / call_oi), 4)

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
out = pd.DataFrame(
    [{
        "date": latest_date,
        "put_oi": float(put_oi),
        "call_oi": float(call_oi),
        "pcr": pcr,
    }]
)

out.to_parquet(OUT_FILE, index=False)

print("✅ PCR BUILD COMPLETE")
print(out)
print(f"💾 Saved : {OUT_FILE}")
