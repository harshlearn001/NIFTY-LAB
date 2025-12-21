#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY EQUITY TO MASTER (SELF-HEALING)

✔ Append-only
✔ Recovers deleted rows from daily history
✔ Master safety lock
✔ Date-safe
✔ Deduplicated
✔ Parquet + CSV
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
DAILY_DIR   = BASE / "data" / "processed" / "daily" / "equity"
MASTER_PQ  = BASE / "data" / "continuous" / "master_equity.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_equity.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | APPEND DAILY EQUITY TO MASTER")
    print("-" * 60)

    # ------------------------------
    # Load ALL daily equity (source of truth)
    # ------------------------------
    daily_files = sorted(DAILY_DIR.glob("EQUITY_NIFTY_*.parquet"))
    if not daily_files:
        print("No daily equity files found — skipping")
        return

    print(f"Daily files found : {len(daily_files)}")

    daily_frames = []
    for f in daily_files:
        df = pd.read_parquet(f)
        df["DATE"] = pd.to_datetime(df["DATE"])
        daily_frames.append(df)

    daily_all = pd.concat(daily_frames, ignore_index=True)

    # ------------------------------
    # Load master (with SAFETY LOCK)
    # ------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master["DATE"] = pd.to_datetime(master["DATE"])

        print(f"Loaded master rows : {len(master):,}")

        # 🔒 CRITICAL SAFETY LOCK
        if len(master) < 100:
            raise RuntimeError(
                "MASTER TOO SMALL — POSSIBLE CORRUPTION. "
                "REFUSING TO MODIFY MASTER EQUITY."
            )
    else:
        master = pd.DataFrame()
        print("No master found — creating new")

    # ------------------------------
    # SELF-HEALING MERGE
    # ------------------------------
    combined = (
        pd.concat([master, daily_all], ignore_index=True)
          .drop_duplicates(subset=["DATE", "SYMBOL"], keep="last")
          .sort_values("DATE")
          .reset_index(drop=True)
    )

    # ------------------------------
    # SAVE
    # ------------------------------
    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER EQUITY UPDATED (SELF-HEALING)")
    print(f"Rows : {len(combined):,}")
    print(f"From : {combined['DATE'].min().date()}")
    print(f"To   : {combined['DATE'].max().date()}")
    print("DONE")

# --------------------------------------------------
if __name__ == "__main__":
    main()
