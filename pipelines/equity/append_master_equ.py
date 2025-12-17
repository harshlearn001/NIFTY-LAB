#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY EQUITY TO MASTER
AUTO • IDEMPOTENT • FAST • FUTURE-PROOF
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR  = BASE / "data" / "processed" / "daily" / "equity"
MASTER_PQ = BASE / "data" / "continuous" / "master_equity.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_equity.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | APPEND DAILY EQUITY TO MASTER")
    print("-" * 60)

    daily_files = sorted(DAILY_DIR.glob("clean_equity_*.parquet"))

    if not daily_files:
        print("No daily equity files found")
        return  # soft exit

    print(f"Daily files found : {len(daily_files)}")

    # ----------------------------------
    # Load master if exists
    # ----------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        print(f"Loaded existing master : {len(master):,} rows")

        last_date = master["DATE"].max().date()
        print(f"Last master date : {last_date}")

    else:
        master = None
        last_date = None
        print("No master found — creating new one")

    # ----------------------------------
    # Load only NEW daily files
    # ----------------------------------
    new_dfs = []

    for f in daily_files:
        df = pd.read_parquet(f)

        df_date = df["DATE"].max().date()

        if last_date and df_date <= last_date:
            continue

        new_dfs.append(df)

    if not new_dfs:
        print("No new data to append")
        return  # idempotent exit

    daily_new = pd.concat(new_dfs, ignore_index=True)
    print(f"New rows loaded : {len(daily_new)}")

    # ----------------------------------
    # Append & deduplicate
    # ----------------------------------
    if master is not None:
        before = len(master)

        master = pd.concat([master, daily_new], ignore_index=True)

        master = (
            master
            .drop_duplicates(subset=["DATE", "SYMBOL"])
            .sort_values("DATE")
            .reset_index(drop=True)
        )

        added = len(master) - before
        print(f"New rows added to master : {added}")

    else:
        master = (
            daily_new
            .drop_duplicates(subset=["DATE", "SYMBOL"])
            .sort_values("DATE")
            .reset_index(drop=True)
        )

    # ----------------------------------
    # Save
    # ----------------------------------
    master.to_parquet(MASTER_PQ, index=False)
    master.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER EQUITY UPDATED")
    print(f"Rows : {len(master):,}")
    print(f"From : {master['DATE'].min().date()}")
    print(f"To   : {master['DATE'].max().date()}")
    print(f"Parquet : {MASTER_PQ}")
    print(f"CSV     : {MASTER_CSV}")
    print("DONE")


if __name__ == "__main__":
    main()
