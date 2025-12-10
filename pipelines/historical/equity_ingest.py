#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ONE-TIME HISTORICAL INGEST
RAW HISTORICAL EQUITY  → MASTER EQUITY
"""

from pathlib import Path
import pandas as pd

# -------------------------------------------------
# PATHS
# -------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

IN_FILE = (
    BASE / "data" / "raw" / "historical" / "equity"
    / "nifty50_equ_hist_2007.csv"
)

OUT_DIR = BASE / "data" / "continuous"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "master_equity.parquet"
OUT_CSV = OUT_DIR / "master_equity.csv"

# -------------------------------------------------
# EXPECTED SCHEMA
# -------------------------------------------------
REQUIRED_COLS = ["DATE", "OPEN", "HIGH", "LOW", "CLOSE"]

# -------------------------------------------------
# MAIN
# -------------------------------------------------
def main():
    print("🚀 HISTORICAL EQUITY INGEST → MASTER")
    print("-" * 70)

    if not IN_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {IN_FILE}")

    # -------------------------------
    # Load
    # -------------------------------
    df = pd.read_csv(IN_FILE)
    print(f"✅ Loaded rows : {len(df):,}")

    # -------------------------------
    # Normalize columns
    # -------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
    )

    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # -------------------------------
    # Parse DATE
    # -------------------------------
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    # -------------------------------
    # Numeric conversion
    # -------------------------------
    for c in ["OPEN", "HIGH", "LOW", "CLOSE"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # -------------------------------
    # Drop junk rows
    # -------------------------------
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLS)
    print(f"🧹 Dropped invalid rows: {before - len(df):,}")

    # -------------------------------
    # Deduplicate + sort
    # -------------------------------
    df = df.drop_duplicates(subset=["DATE"])
    df = df.sort_values("DATE").reset_index(drop=True)

    # -------------------------------
    # Save MASTER
    # -------------------------------
    df.to_parquet(OUT_PQ, index=False)
    df.to_csv(OUT_CSV, index=False)

    print("-" * 70)
    print("✅ MASTER EQUITY CREATED")
    print(f"📊 Rows       : {len(df):,}")
    print(f"📅 From       : {df['DATE'].min().date()}")
    print(f"📅 To         : {df['DATE'].max().date()}")
    print(f"💾 Parquet    : {OUT_PQ}")
    print(f"💾 CSV        : {OUT_CSV}")
    print("🎉 DONE (RUN ONCE ONLY) ✅")

if __name__ == "__main__":
    main()
