#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Daily NIFTY Options PCR Builder
-------------------------------
Builds Put–Call Ratio (PCR) using nearest expiry options OI.
Robust to NSE schema variations.
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # H:\NIFTY-LAB
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
OPT_DAILY_DIR = PROC_DIR / "daily" / "options"
OUT_DIR = PROC_DIR / "options_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_pcr_daily.parquet"

# =================================================
# LOAD FILES
# =================================================
files = sorted(OPT_DAILY_DIR.glob("OPTIONS_NIFTY_*.parquet"))
if not files:
    raise FileNotFoundError("❌ No options daily files found")

records = []

print(f"📥 Processing {len(files)} option files...")

# =================================================
# PROCESS EACH FILE
# =================================================
for file in files:
    df = pd.read_parquet(file)

    # ---------------- DATE ----------------
    if "TRADE_DATE" in df.columns:
        df["date"] = pd.to_datetime(df["TRADE_DATE"])
    elif "DATE" in df.columns:
        df["date"] = pd.to_datetime(df["DATE"])
    else:
        raise ValueError(f"❌ No trade date column in {file.name}")

    # ---------------- EXPIRY ----------------
    if "EXPIRY_DT" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXPIRY_DT"])
    elif "EXP_DATE" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXP_DATE"])
    elif "EXPIRY_DATE" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXPIRY_DATE"])
    else:
        raise ValueError(f"❌ No expiry column in {file.name}")

    # ---------------- OPEN INTEREST ----------------
    if "OPEN_INT" in df.columns:
        df["open_interest"] = df["OPEN_INT"]
    elif "OPEN_INTEREST" in df.columns:
        df["open_interest"] = df["OPEN_INTEREST"]
    else:
        raise ValueError(f"❌ No open interest column in {file.name}")

    # ---------------- OPTION TYPE DETECTION (KEY FIX) ----------------
    df["opt_type"] = None

    # 1️⃣ If any column contains CE/PE values
    for col in df.columns:
        if df[col].dtype == object:
            mask_ce = df[col].astype(str).str.contains("CE", regex=False)
            mask_pe = df[col].astype(str).str.contains("PE", regex=False)
            df.loc[mask_ce, "opt_type"] = "CE"
            df.loc[mask_pe, "opt_type"] = "PE"

    # 2️⃣ If still missing, try STRIKE + CE/PE pattern
    if df["opt_type"].isna().all():
        raise ValueError(
            f"❌ Cannot detect option type (CE/PE) in {file.name}. "
            f"Inspect columns: {df.columns.tolist()}"
        )

    trade_date = df["date"].iloc[0]

    # ---------------- NEAREST EXPIRY ----------------
    df = df.sort_values("expiry")
    nearest_expiry = df["expiry"].iloc[0]
    df = df[df["expiry"] == nearest_expiry]

    # ---------------- AGGREGATE OI ----------------
    put_oi = df.loc[df["opt_type"] == "PE", "open_interest"].sum()
    call_oi = df.loc[df["opt_type"] == "CE", "open_interest"].sum()

    if call_oi == 0:
        continue

    records.append({
        "date": trade_date,
        "expiry": nearest_expiry,
        "total_put_oi": put_oi,
        "total_call_oi": call_oi,
        "pcr": round(put_oi / call_oi, 3),
    })

# =================================================
# FINAL OUTPUT
# =================================================
df_out = pd.DataFrame(records).sort_values("date")

df_out.to_parquet(OUT_FILE, index=False)
df_out.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

# =================================================
# SUMMARY
# =================================================
print("\n✅ NIFTY PCR built successfully")
print(f"📦 Parquet: {OUT_FILE}")
print(f"📄 CSV: {OUT_FILE.with_suffix('.csv')}")
print("📅 Date range:", df_out['date'].min().date(), "→", df_out['date'].max().date())
print("\n📊 PCR Stats")
print(df_out["pcr"].describe().round(3))
