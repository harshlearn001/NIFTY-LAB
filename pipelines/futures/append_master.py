#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY NIFTY FUTURES TO MASTER

Input  : data/processed/daily/futures/FUT_NIFTY_YYYY-MM-DD.parquet
Output : data/continuous/master_futures.parquet

AUTO • APPEND-ONLY • DUPLICATE SAFE
(NIFTY ONLY — SYMBOL REMOVED)
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR   = BASE / "data" / "processed" / "daily" / "futures"
MASTER_PQ  = BASE / "data" / "continuous" / "master_futures.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_futures.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | APPEND DAILY NIFTY FUTURES TO MASTER")
    print("-" * 60)

    daily_files = sorted(DAILY_DIR.glob("FUT_NIFTY_*.parquet"))

    if not daily_files:
        print("No daily NIFTY futures files found")
        return  # soft exit

    print(f"Daily files found : {len(daily_files)}")

    # --------------------------------------------------
    # Load existing master
    # --------------------------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        print(f"Loaded existing master : {len(master):,} rows")
        existing_dates = set(master["TRADE_DATE"].unique())
    else:
        master = pd.DataFrame()
        existing_dates = set()
        print("Master does not exist — creating new one")

    # --------------------------------------------------
    # Load only NEW trade dates
    # --------------------------------------------------
    new_rows = []

    for f in daily_files:
        df = pd.read_parquet(f)

        df = df[~df["TRADE_DATE"].isin(existing_dates)]

        if df.empty:
            continue

        new_rows.append(df)

    if not new_rows:
        print("No new rows to append")
        return  # idempotent exit

    new_df = pd.concat(new_rows, ignore_index=True)
    print(f"New rows loaded : {len(new_df)}")

    # --------------------------------------------------
    # Append + final dedupe
    # --------------------------------------------------
    if master.empty:
        final = new_df
    else:
        final = pd.concat([master, new_df], ignore_index=True)

    final = (
        final
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .drop_duplicates(
            subset=["TRADE_DATE", "EXP_DATE"],
            keep="last"
        )
        .reset_index(drop=True)
    )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    final.to_parquet(MASTER_PQ, index=False)
    final.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER FUTURES UPDATED")
    print(f"Rows : {len(final):,}")
    print(f"From : {final['TRADE_DATE'].min().date()}")
    print(f"To   : {final['TRADE_DATE'].max().date()}")
    print(f"Parquet : {MASTER_PQ}")
    print(f"CSV     : {MASTER_CSV}")
    print("DONE")


if __name__ == "__main__":
    main()
