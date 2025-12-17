#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY EQUITY TO MASTER

✔ Picks EQUITY_NIFTY_*.parquet
✔ Date-safe
✔ Idempotent
✔ Historical compatible
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

    daily_files = sorted(DAILY_DIR.glob("EQUITY_NIFTY_*.parquet"))

    if not daily_files:
        print("No daily equity files found")
        return

    print(f"Daily files found : {len(daily_files)}")

    # ------------------------------
    # Load master
    # ------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master["DATE"] = pd.to_datetime(master["DATE"], dayfirst=True)
        last_date = master["DATE"].max()
        print(f"Loaded master : {len(master):,} rows")
        print(f"Last master date : {last_date.date()}")
    else:
        master = pd.DataFrame()
        last_date = None
        print("No master found — creating new")

    # ------------------------------
    # Load only NEW data
    # ------------------------------
    new_rows = []

    for f in daily_files:
        df = pd.read_parquet(f)
        df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True)

        if last_date is not None:
            df = df[df["DATE"] > last_date]

        if not df.empty:
            new_rows.append(df)

    if not new_rows:
        print("No new data to append")
        return

    daily_new = pd.concat(new_rows, ignore_index=True)
    print(f"New rows loaded : {len(daily_new)}")

    # ------------------------------
    # Append & dedupe
    # ------------------------------
    final = (
        pd.concat([master, daily_new], ignore_index=True)
        .drop_duplicates(subset=["DATE", "SYMBOL"])
        .sort_values("DATE")
        .reset_index(drop=True)
    )

    # ------------------------------
    # SAVE
    # ------------------------------
    final.to_parquet(MASTER_PQ, index=False)
    final.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER EQUITY UPDATED")
    print(f"Rows : {len(final):,}")
    print(f"From : {final['DATE'].min().date()}")
    print(f"To   : {final['DATE'].max().date()}")
    print("DONE")

# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":
    main()
