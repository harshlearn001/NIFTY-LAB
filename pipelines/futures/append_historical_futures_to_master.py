#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND HISTORICAL FUTURES TO MASTER

✔ Input  : historical futures parquet
✔ Output : master_futures.parquet / csv
✔ Append-only (safe)
✔ OI → OPEN_INTEREST normalized
✔ Numeric sanity enforced
✔ Deduplicated
"""

from pathlib import Path
import pandas as pd

# ==================================================
# PATHS
# ==================================================
BASE = Path(r"H:\NIFTY-LAB")

HIST_PQ   = BASE / "data" / "raw" / "historical" / "futures" / "nifty50_future_hist_2016.parquet"
MASTER_PQ  = BASE / "data" / "continuous" / "master_futures.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_futures.csv"

# ==================================================
# NUMERIC SCHEMA
# ==================================================
FLOAT_COLS = ["OPEN", "HIGH", "LOW", "CLOSE"]
INT_COLS   = ["VOLUME", "OPEN_INTEREST"]

# ==================================================
# HELPERS
# ==================================================
def sanitize_futures_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize OI column
    if "OI" in df.columns and "OPEN_INTEREST" not in df.columns:
        df = df.rename(columns={"OI": "OPEN_INTEREST"})

    # Clean junk / commas
    for col in FLOAT_COLS + INT_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace({"": None, "-": None})
            )

    # Prices → float
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Counts → nullable int
    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df

# ==================================================
# MAIN
# ==================================================
def main():
    print("NIFTY-LAB | APPEND HISTORICAL FUTURES TO MASTER")
    print("-" * 60)

    # --------------------------------------------------
    # Load historical futures
    # --------------------------------------------------
    if not HIST_PQ.exists():
        raise FileNotFoundError(f"Historical file not found: {HIST_PQ}")

    hist = pd.read_parquet(HIST_PQ)
    hist.columns = [c.strip().upper() for c in hist.columns]

    required = {
        "SYMBOL", "TRADE_DATE", "EXP_DATE",
        "OPEN", "HIGH", "LOW", "CLOSE",
        "VOLUME", "OI"
    }
    if not required.issubset(hist.columns):
        raise RuntimeError(f"Schema mismatch in historical file: {hist.columns}")

    # Dates
    hist["TRADE_DATE"] = pd.to_datetime(hist["TRADE_DATE"])
    hist["EXP_DATE"] = pd.to_datetime(hist["EXP_DATE"])

    # Numeric sanity
    hist = sanitize_futures_numeric(hist)

    print(f"Historical rows loaded : {len(hist):,}")
    print(f"Historical range      : {hist['TRADE_DATE'].min().date()} → {hist['TRADE_DATE'].max().date()}")

    # --------------------------------------------------
    # Load existing master (if any)
    # --------------------------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master["TRADE_DATE"] = pd.to_datetime(master["TRADE_DATE"])
        master["EXP_DATE"] = pd.to_datetime(master["EXP_DATE"])
        print(f"Existing master rows  : {len(master):,}")
    else:
        master = pd.DataFrame()
        print("No master found — creating new")

    # --------------------------------------------------
    # Append + dedupe
    # --------------------------------------------------
    combined = (
        pd.concat([master, hist], ignore_index=True)
        .drop_duplicates(subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"], keep="last")
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .reset_index(drop=True)
    )

    # --------------------------------------------------
    # SAVE
    # --------------------------------------------------
    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER FUTURES UPDATED (HISTORICAL APPENDED)")
    print(f"Total rows : {len(combined):,}")
    print(f"From       : {combined['TRADE_DATE'].min().date()}")
    print(f"To         : {combined['TRADE_DATE'].max().date()}")
    print("DONE")

# ==================================================
# ENTRY
# ==================================================
if __name__ == "__main__":
    main()
