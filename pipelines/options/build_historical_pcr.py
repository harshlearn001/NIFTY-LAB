#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Historical NIFTY Options PCR Builder
-----------------------------------
Builds daily Put–Call Ratio (PCR) from historical options OI data
and merges it with existing daily PCR.

Output:
- data/processed/options_ml/nifty_pcr_daily.parquet
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
from configs.paths import RAW_DIR, PROC_DIR

# =================================================
# PATHS
# =================================================
HIST_DIR = RAW_DIR / "historical" / "options"
OUT_DIR = PROC_DIR / "options_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_pcr_daily.parquet"

# =================================================
# LOAD HISTORICAL FILES
# =================================================
files = list(HIST_DIR.glob("*.parquet")) + list(HIST_DIR.glob("*.csv"))

if not files:
    raise FileNotFoundError("❌ No historical options files found")

records = []

print(f"📥 Processing {len(files)} historical options files...")

# =================================================
# PROCESS EACH FILE
# =================================================
for file in files:
    print(f"   → {file.name}")

    if file.suffix == ".parquet":
        df = pd.read_parquet(file)
    else:
        df = pd.read_csv(file)

    # ---------------- DATE ----------------
    if "TRADE_DATE" in df.columns:
        df["date"] = pd.to_datetime(df["TRADE_DATE"])
    elif "DATE" in df.columns:
        df["date"] = pd.to_datetime(df["DATE"])
    else:
        raise ValueError("❌ No trade date column")

    # ---------------- EXPIRY ----------------
    if "EXPIRY_DT" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXPIRY_DT"])
    elif "EXP_DATE" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXP_DATE"])
    elif "EXPIRY_DATE" in df.columns:
        df["expiry"] = pd.to_datetime(df["EXPIRY_DATE"])
    else:
        raise ValueError("❌ No expiry column")

    # ---------------- OPEN INTEREST ----------------
    if "OPEN_INT" in df.columns:
        df["open_interest"] = df["OPEN_INT"]
    elif "OPEN_INTEREST" in df.columns:
        df["open_interest"] = df["OPEN_INTEREST"]
    else:
        raise ValueError("❌ No open interest column")

    # ---------------- FILTER NIFTY ----------------
    if "SYMBOL" in df.columns:
        df = df[df["SYMBOL"].str.upper() == "NIFTY"]

    # ---------------- OPTION TYPE DETECTION ----------------
    df["opt_type"] = None

    for col in df.columns:
        if df[col].dtype == object:
            ce_mask = df[col].astype(str).str.contains("CE", regex=False)
            pe_mask = df[col].astype(str).str.contains("PE", regex=False)
            df.loc[ce_mask, "opt_type"] = "CE"
            df.loc[pe_mask, "opt_type"] = "PE"

    if df["opt_type"].isna().all():
        raise ValueError(f"❌ Could not detect CE/PE in {file.name}")

    # =================================================
    # BUILD PCR PER DAY
    # =================================================
    for date, dfd in df.groupby("date"):
        dfd = dfd.sort_values("expiry")
        nearest_expiry = dfd["expiry"].iloc[0]
        dfd = dfd[dfd["expiry"] == nearest_expiry]

        put_oi = dfd.loc[dfd["opt_type"] == "PE", "open_interest"].sum()
        call_oi = dfd.loc[dfd["opt_type"] == "CE", "open_interest"].sum()

        if call_oi == 0:
            continue

        records.append({
            "date": date,
            "expiry": nearest_expiry,
            "total_put_oi": put_oi,
            "total_call_oi": call_oi,
            "pcr": round(put_oi / call_oi, 3),
        })

# =================================================
# BUILD HISTORICAL PCR DF
# =================================================
hist_df = pd.DataFrame(records)
hist_df = hist_df.sort_values("date").drop_duplicates("date")

# =================================================
# MERGE WITH EXISTING DAILY PCR
# =================================================
if OUT_FILE.exists():
    daily_df = pd.read_parquet(OUT_FILE)
    final_df = pd.concat([daily_df, hist_df], ignore_index=True)
    final_df = final_df.sort_values("date").drop_duplicates("date")
else:
    final_df = hist_df

# =================================================
# SAVE
# =================================================
final_df.to_parquet(OUT_FILE, index=False)
final_df.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

# =================================================
# SUMMARY
# =================================================
print("\n✅ HISTORICAL PCR BUILD COMPLETE")
print(f"📦 File: {OUT_FILE}")
print("📅 Date range:", final_df['date'].min().date(), "→", final_df['date'].max().date())
print("📊 Total PCR days:", len(final_df))
print("\n📊 PCR Stats")
print(final_df["pcr"].describe().round(3))
