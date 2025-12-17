#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY OPTIONS (NIFTY ONLY)

AUTO MODE — SAFE FOR SCHEDULER

- NSE-safe spacing fix
- Handles junk headers
- ZIP filename -> TRADE_DATE
- OPTIDX + NIFTY only
- Dates kept as datetime64[ns]
"""

from pathlib import Path
from datetime import datetime
import argparse
import zipfile
import pandas as pd
from io import StringIO

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

RAW_DIR = BASE / "data" / "raw" / "options"
OUT_DIR = BASE / "data" / "processed" / "daily" / "options"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def read_nse_csv(f):
    """Skip junk header lines until the INSTRUMENT row."""
    lines = f.read().decode("utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("INSTRUMENT"):
            return pd.read_csv(StringIO("\n".join(lines[i:])))
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace("*", "", regex=False)
    )
    return df


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main(trade_date: datetime.date):
    print("NIFTY-LAB | CLEAN DAILY OPTIONS (AUTO | NIFTY ONLY)")
    print("-" * 60)
    print(f"Trade Date : {trade_date}")

    # Weekend guard
    if trade_date.weekday() >= 5:
        print("Weekend detected — NSE FO closed")
        return

    zip_file = RAW_DIR / f"fo_{trade_date:%Y-%m-%d}.zip"
    out_pq = OUT_DIR / f"OPTIONS_NIFTY_{trade_date}.parquet"
    out_csv = OUT_DIR / f"OPTIONS_NIFTY_{trade_date}.csv"

    if not zip_file.exists():
        print(f"FO ZIP not found : {zip_file.name}")
        return

    if out_pq.exists() and out_csv.exists():
        print(f"Already cleaned : {out_pq.name}")
        return

    frames = []

    print(f"Processing : {zip_file.name}")

    with zipfile.ZipFile(zip_file) as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue

            with z.open(name) as f:
                df = read_nse_csv(f)
                if df is None or df.empty:
                    continue

                df = normalize_columns(df)

                if not {"INSTRUMENT", "SYMBOL"}.issubset(df.columns):
                    continue

                # NSE-safe filters
                df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip().str.upper()
                df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()

                df = df[
                    (df["INSTRUMENT"] == "OPTIDX") &
                    (df["SYMBOL"] == "NIFTY")
                ]

                if df.empty:
                    continue

                # Expiry column detection
                for c in ("EXP_DATE", "EXPIRY", "EXPIRY_DATE", "EXP_DT"):
                    if c in df.columns:
                        df.rename(columns={c: "EXP_DATE"}, inplace=True)
                        break
                else:
                    continue

                df["TRADE_DATE"] = trade_date
                frames.append(df)

    if not frames:
        print("No valid NIFTY options found")
        return

    df = pd.concat(frames, ignore_index=True)

    # --------------------------------------------------
    # Date normalization
    # --------------------------------------------------
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], errors="coerce")
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")

    # --------------------------------------------------
    # Numeric columns only
    # --------------------------------------------------
    numeric_cols = [
        "STR_PRICE",
        "OPEN_PRICE",
        "HI_PRICE",
        "LO_PRICE",
        "CLOSE_PRICE",
        "OPEN_INT",
        "TRD_QTY",
        "NO_OF_CONT",
        "NO_OF_TRADE",
        "NOTION_VAL",
        "PR_VAL",
    ]

    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # --------------------------------------------------
    # Final clean
    # --------------------------------------------------
    df = (
        df.dropna(subset=["TRADE_DATE", "EXP_DATE", "STR_PRICE"])
          .sort_values(["TRADE_DATE", "EXP_DATE", "STR_PRICE", "OPT_TYPE"])
          .drop_duplicates()
    )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)

    print(f"Saved {out_pq.name} | rows : {len(df):,}")
    print("DAILY OPTIONS CLEAN COMPLETE")


# --------------------------------------------------
# CLI
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean daily NSE NIFTY options data"
    )
    parser.add_argument(
        "--date",
        help="Trade date YYYY-MM-DD (default: today)",
        required=False,
    )

    args = parser.parse_args()

    trade_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date
        else datetime.today().date()
    )

    main(trade_date)
