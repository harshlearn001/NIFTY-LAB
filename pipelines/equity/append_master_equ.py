#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY EQUITY TO MASTER
SAFE • IDEMPOTENT • RERUN FRIENDLY
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR = BASE / "data" / "processed" / "daily" / "equity"
MASTER_PQ = BASE / "data" / "continuous" / "master_equity.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_equity.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print("🚀 NIFTY-LAB | APPEND DAILY EQUITY → MASTER")
    print("-" * 60)

    files = sorted(DAILY_DIR.glob("clean_equity_*.parquet"))

    if not files:
        print("⚠️ No daily equity files found")
        return

    print(f"📄 Daily files found: {len(files)}")

    daily_all = []
    for f in files:
        df = pd.read_parquet(f)
        daily_all.append(df)

    daily_df = pd.concat(daily_all, ignore_index=True)

    # ----------------------------------
    # Load or create master
    # ----------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        print(f"✅ Loaded existing master: {len(master):,} rows")

        before = len(master)

        master = pd.concat([master, daily_df], ignore_index=True)

        master = (
            master
            .drop_duplicates(subset=["DATE"])
            .sort_values("DATE")
            .reset_index(drop=True)
        )

        added = len(master) - before
        print(f"➕ New rows added: {added}")

    else:
        master = (
            daily_df
            .sort_values("DATE")
            .drop_duplicates(subset=["DATE"])
            .reset_index(drop=True)
        )
        print("✅ Created new master file")

    # ----------------------------------
    # Save
    # ----------------------------------
    master.to_parquet(MASTER_PQ, index=False)
    master.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("✅ MASTER EQUITY UPDATED")
    print(f"📊 Rows : {len(master):,}")
    print(f"📅 From : {master['DATE'].min().date()}")
    print(f"📅 To   : {master['DATE'].max().date()}")
    print(f"💾 Parquet : {MASTER_PQ}")
    print("🎉 DONE ✅")


if __name__ == "__main__":
    main()
