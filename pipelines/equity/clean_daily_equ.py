#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY EQUITY DATA
AUTO MODE — PARQUET + CSV

Input  : data/raw/equity/equity_YYYY-MM-DD.csv
Output :
  - data/processed/daily/equity/clean_equity_YYYY-MM-DD.parquet
  - data/processed/daily/equity/clean_equity_YYYY-MM-DD.csv
"""

from pathlib import Path
from datetime import datetime
import argparse
import pandas as pd

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
def main(trade_date: datetime):
    date_str = trade_date.strftime("%Y-%m-%d")

    print("NIFTY-LAB | CLEAN DAILY EQUITY (AUTO)")
    print("-" * 60)
    print(f"Trade Date : {date_str}")

    raw_file = RAW_DIR / f"equity_{date_str}.csv"
    parquet_file = OUT_DIR / f"clean_equity_{date_str}.parquet"
    csv_file = OUT_DIR / f"clean_equity_{date_str}.csv"

    # ------------------------------
    # Checks
    # ------------------------------
    if not raw_file.exists():
        print(f"Raw file not found -> {raw_file.name}")
        return  # soft exit

    if parquet_file.exists() and csv_file.exists():
        print(f"Already cleaned -> {parquet_file.name} & CSV")
        return  # idempotent exit

    # ------------------------------
    # Load
    # ------------------------------
    df = pd.read_csv(raw_file)
    print(f"Loaded rows : {len(df)}")

    # ------------------------------
    # Standardize columns
    # ------------------------------
    df.columns = df.columns.str.strip().str.upper()

    REQUIRED = {"DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"}
    missing = REQUIRED - set(df.columns)
    if missing:
        print(f"Missing columns: {missing}")
        return  # soft exit

    # ------------------------------
    # Datatypes
    # ------------------------------
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------
    # Add SYMBOL (single-instrument equity)
    # ------------------------------
    df["SYMBOL"] = "NIFTY"

    # ------------------------------
    # Sanity filters
    # ------------------------------
    before = len(df)

    df = df.dropna()
    df = df[df["VOLUME"] >= 0]

    after = len(df)
    print(f"Dropped rows : {before - after}")

    if df.empty:
        print("No valid data after cleaning")
        return  # soft exit

    # ------------------------------
    # Sort & Save
    # ------------------------------
    df = df.sort_values("DATE")

    df.to_parquet(parquet_file, index=False)
    df.to_csv(csv_file, index=False)

    print("Clean file created")
    print(f"Rows    : {len(df)}")
    print(f"Parquet : {parquet_file}")
    print(f"CSV     : {csv_file}")
    print("DONE")


# --------------------------------------------------
# CLI
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean daily NIFTY equity data"
    )
    parser.add_argument(
        "--date",
        help="Trade date in YYYY-MM-DD format (default: today)",
        required=False,
    )

    args = parser.parse_args()

    trade_date = (
        datetime.strptime(args.date, "%Y-%m-%d")
        if args.date
        else datetime.today()
    )

    main(trade_date)
