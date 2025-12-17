#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY FUTURES (NIFTY ONLY)

AUTO MODE — SAFE FOR SCHEDULER

- NSE-safe spacing fix
- Handles nanosecond dates
- Extracts ONLY NIFTY index futures
- Produces ML + master-ready output
"""

from pathlib import Path
from datetime import datetime
import argparse
import zipfile
import pandas as pd
import re
from io import StringIO

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

RAW_DIR = BASE / "data" / "raw" / "futures"
OUT_DIR = BASE / "data" / "processed" / "daily" / "futures"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
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


def read_nse_csv(f):
    lines = f.read().decode("utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("INSTRUMENT"):
            return pd.read_csv(StringIO("\n".join(lines[i:])))
    return None


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main(trade_date: datetime.date):
    print("NIFTY-LAB | CLEAN DAILY FUTURES (AUTO | NIFTY ONLY)")
    print("-" * 60)
    print(f"Trade Date : {trade_date}")

    if trade_date.weekday() >= 5:
        print("Weekend detected — no FO futures")
        return

    zip_file = RAW_DIR / f"fo_{trade_date:%Y-%m-%d}.zip"
    out_pq = OUT_DIR / f"FUT_NIFTY_{trade_date}.parquet"
    out_csv = OUT_DIR / f"FUT_NIFTY_{trade_date}.csv"

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

                # NSE spacing fix
                df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip().str.upper()
                df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip().str.upper()

                # NIFTY futures only
                df = df[
                    (df["INSTRUMENT"] == "FUTIDX") &
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
        print("No valid NIFTY futures found")
        return

    df = pd.concat(frames, ignore_index=True)

    # --------------------------------------------------
    # Final schema normalization
    # --------------------------------------------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True, errors="coerce")

    df.rename(
        columns={
            "OPEN_PRICE": "OPEN",
            "HI_PRICE": "HIGH",
            "LO_PRICE": "LOW",
            "CLOSE_PRICE": "CLOSE",
            "TRD_QTY": "VOLUME",
            "OPEN_INT": "OI",
        },
        inplace=True,
    )

    df = df[
        [
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
    ]

    for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "OI"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = (
        df.dropna()
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .drop_duplicates()
    )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)

    print(f"Saved {out_pq.name} | rows : {len(df)}")
    print("DAILY FUTURES CLEAN COMPLETE")


# --------------------------------------------------
# CLI
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean daily NSE NIFTY futures data"
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
