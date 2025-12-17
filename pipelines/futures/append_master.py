#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY NIFTY FUTURES TO MASTER

✔ Date-safe (CSV + Parquet aligned)
✔ Append-only
✔ Duplicate-proof
✔ Production ready
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
        print("No daily futures files found")
        return

    print(f"Daily files found : {len(daily_files)}")

    # --------------------------------------------------
    # LOAD MASTER (SAFE)
    # --------------------------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        print(f"Loaded existing master : {len(master):,} rows")
        existing_keys = set(
            zip(master["SYMBOL"], master["TRADE_DATE"], master["EXP_DATE"])
        )
    else:
        master = pd.DataFrame()
        existing_keys = set()
        print("Master does not exist — creating new one")

    # --------------------------------------------------
    # LOAD ONLY NEW ROWS
    # --------------------------------------------------
    new_rows = []

    for f in daily_files:
        df = pd.read_parquet(f)

        df["_key"] = list(zip(df["SYMBOL"], df["TRADE_DATE"], df["EXP_DATE"]))
        df = df[~df["_key"].isin(existing_keys)]
        df = df.drop(columns="_key")

        if not df.empty:
            new_rows.append(df)

    if not new_rows:
        print("No new rows to append")
        return

    new_df = pd.concat(new_rows, ignore_index=True)
    print(f"New rows added : {len(new_df)}")

    final = pd.concat([master, new_df], ignore_index=True)

    final = (
        final.sort_values(["TRADE_DATE", "EXP_DATE"])
             .drop_duplicates(subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"], keep="last")
             .reset_index(drop=True)
    )

    final.to_parquet(MASTER_PQ, index=False)
    final.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER FUTURES UPDATED")
    print(f"Rows : {len(final):,}")
    print(f"From : {final['TRADE_DATE'].min().date()}")
    print(f"To   : {final['TRADE_DATE'].max().date()}")
    print("DONE")


if __name__ == "__main__":
    main()
