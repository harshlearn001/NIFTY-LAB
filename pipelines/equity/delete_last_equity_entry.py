#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DELETE LAST EQUITY ENTRY FROM MASTER

✔ Date-safe
✔ Deletes latest DATE rows
✔ Updates Parquet + CSV
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
MASTER_PQ  = BASE / "data" / "continuous" / "master_equity.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_equity.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | DELETE LAST EQUITY ENTRY")
    print("-" * 60)

    if not MASTER_PQ.exists():
        print("❌ master_equity.parquet not found")
        return

    df = pd.read_parquet(MASTER_PQ)

    # 🔑 Critical: normalize DATE
    df["DATE"] = pd.to_datetime(df["DATE"], dayfirst=True, errors="coerce")

    last_date = df["DATE"].max()

    print(f"Latest DATE in master : {last_date.date()}")

    before = len(df)

    # ❌ Remove latest date rows
    df = df[df["DATE"] < last_date]

    after = len(df)

    removed = before - after

    if removed == 0:
        print("Nothing removed")
        return

    # 💾 SAVE
    df.to_parquet(MASTER_PQ, index=False)
    df.to_csv(MASTER_CSV, index=False)

    print(f"✅ Rows removed : {removed}")
    print(f"Remaining rows : {after}")
    print("MASTER EQUITY UPDATED")

# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":
    main()
