#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
HISTORICAL FUTURES INGEST (RUN ONCE ONLY)

Purpose:
- Load cleaned historical futures data
- Enforce schema + types
- Create MASTER FUTURES dataset
- Save into /data/continuous (single source of truth)

⚠️ RUN ONCE ONLY
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------

BASE = Path(r"H:\NIFTY-LAB")

IN_FILE = (
    BASE
    / "data"
    / "raw"
    / "historical"
    / "futures"
    / "nifty50_future_hist_2016.parquet"
)

# ✅ OUTPUT MOVED TO CONTINUOUS
OUT_DIR = BASE / "data" / "continuous"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "master_futures.parquet"
OUT_CSV = OUT_DIR / "master_futures.csv"

# --------------------------------------------------
# EXPECTED SCHEMA
# --------------------------------------------------

REQUIRED_COLS = [
    "SYMBOL",
    "TRADE_DATE",
    "EXP_DATE",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "VOLUME",
    "OI",
]

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print("🚀 HISTORICAL FUTURES INGEST → MASTER (CONTINUOUS)")
    print("-" * 70)

    # ---------------- RUN-ONCE SAFETY ----------------
    if OUT_PQ.exists():
        raise RuntimeError(
            f"\n❌ MASTER FUTURES already exists:\n{OUT_PQ}\n\n"
            "This job is RUN-ONCE ONLY.\n"
            "Delete manually if rebuild is intentional.\n"
        )

    if not IN_FILE.exists():
        raise FileNotFoundError(f"❌ Input not found:\n{IN_FILE}")

    # ---------------- LOAD ----------------
    df = pd.read_parquet(IN_FILE)
    print(f"✅ Loaded rows : {len(df):,}")

    # ---------------- NORMALIZE COLUMNS ----------------
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
    )

    # ---------------- SCHEMA CHECK ----------------
    missing = set(REQUIRED_COLS) - set(df.columns)
    if missing:
        raise ValueError(f"❌ Missing required columns: {sorted(missing)}")

    df = df[REQUIRED_COLS]

    # ---------------- TYPE ENFORCEMENT ----------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"], errors="coerce")

    num_cols = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "OI"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # ---------------- CLEAN ----------------
    before = len(df)
    df = df.dropna(subset=["TRADE_DATE", "EXP_DATE"])
    df = df[df["VOLUME"] >= 0]
    dropped = before - len(df)

    print(f"🧹 Dropped invalid rows: {dropped:,}")

    # ---------------- SORT & DEDUP ----------------
    df = (
        df
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # ---------------- SAVE ----------------
    df.to_parquet(OUT_PQ, index=False)
    df.to_csv(OUT_CSV, index=False)

    # ---------------- SUMMARY ----------------
    print("-" * 70)
    print("✅ MASTER FUTURES CREATED (CONTINUOUS)")
    print(f"📊 Rows       : {len(df):,}")
    print(f"📅 From       : {df['TRADE_DATE'].min().date()}")
    print(f"📅 To         : {df['TRADE_DATE'].max().date()}")
    print(f"💾 Parquet    : {OUT_PQ}")
    print(f"💾 CSV        : {OUT_CSV}")
    print("🎉 DONE (RUN ONCE ONLY) ✅")


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------

if __name__ == "__main__":
    main()
