#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY EQUITY DATA
Input  : data/raw/equity/equity_YYYY-MM-DD.csv
Output : data/processed/daily/equity/clean_equity_YYYY-MM-DD.parquet
"""

from pathlib import Path
import pandas as pd
from datetime import datetime

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

RAW_DIR = BASE / "data" / "raw" / "equity"
OUT_DIR = BASE / "data" / "processed" / "daily" / "equity"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print("🧹 NIFTY-LAB | CLEAN DAILY EQUITY")
    print("-" * 60)

    date_str = input(
        "📅 Enter date to clean (YYYY-MM-DD): "
    ).strip()

    try:
        trade_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format")
        return

    raw_file = RAW_DIR / f"equity_{date_str}.csv"
    out_file = OUT_DIR / f"clean_equity_{date_str}.parquet"

    if not raw_file.exists():
        print(f"❌ Raw file not found: {raw_file.name}")
        return

    if out_file.exists():
        print(f"⚠️ Already cleaned: {out_file.name}")
        return

    # ------------------------------
    # Load
    # ------------------------------
    df = pd.read_csv(raw_file)
    print(f"✅ Loaded rows : {len(df)}")

    # ------------------------------
    # Standardize columns
    # ------------------------------
    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
    )

    REQUIRED = {"DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"}
    missing = REQUIRED - set(df.columns)

    if missing:
        print(f"❌ Missing columns: {missing}")
        return

    # ------------------------------
    # Datatypes
    # ------------------------------
    df["DATE"] = pd.to_datetime(df["DATE"])
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------
    # Add SYMBOL
    # ------------------------------
    df["SYMBOL"] = "NIFTY"

    # ------------------------------
    # Sanity filters
    # ------------------------------
    before = len(df)

    df = df.dropna()
    df = df[df["VOLUME"] >= 0]

    after = len(df)
    print(f"🧹 Dropped rows : {before - after}")

    if df.empty:
        print("❌ No valid data after cleaning")
        return

    # ------------------------------
    # Sort & Save
    # ------------------------------
    df = df.sort_values("DATE")

    df.to_parquet(out_file, index=False)

    print("✅ CLEAN FILE CREATED")
    print(f"📅 Date : {date_str}")
    print(f"📦 Rows : {len(df)}")
    print(f"💾 File : {out_file}")
    print("🎉 DONE ✅")


if __name__ == "__main__":
    main()
