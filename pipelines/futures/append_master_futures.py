#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY FUTURES TO MASTER (LOCKED + SELF-HEALING)

✔ Append-only (date-aware)
✔ Numeric sanity enforced
✔ OI → OPEN_INTEREST normalized
✔ MASTER SAFETY LOCK
✔ Auto-backup before write
✔ Deduplicated & sorted
✔ Scheduler-safe
"""

from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime

# ==================================================
# PATHS
# ==================================================
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR   = BASE / "data" / "processed" / "daily" / "futures"
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

    # Clean junk
    for col in FLOAT_COLS + INT_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .replace({"": None, "-": None})
            )

    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df

# ==================================================
# MAIN
# ==================================================
def main():
    print("NIFTY-LAB | APPEND DAILY FUTURES TO MASTER")
    print("-" * 60)

    # --------------------------------------------------
    # Load master (WITH LOCK)
    # --------------------------------------------------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master["TRADE_DATE"] = pd.to_datetime(master["TRADE_DATE"])
        master["EXP_DATE"] = pd.to_datetime(master["EXP_DATE"])

        print(f"Loaded master rows : {len(master):,}")

        # 🔒 HARD SAFETY LOCK
        if len(master) < 100:
            raise RuntimeError(
                "MASTER FUTURES TOO SMALL — POSSIBLE CORRUPTION. "
                "REFUSING TO MODIFY."
            )

        last_date = master["TRADE_DATE"].max()
        print(f"Last master date  : {last_date.date()}")

    else:
        master = pd.DataFrame()
        last_date = None
        print("No master found — creating new")

    # --------------------------------------------------
    # Load truly new daily data
    # --------------------------------------------------
    new_rows = []

    for file in sorted(DAILY_DIR.glob("FUT_NIFTY_*.csv")):
        df = pd.read_csv(file)
        df.columns = [c.strip().upper() for c in df.columns]

        required = {
            "SYMBOL", "TRADE_DATE", "EXP_DATE",
            "OPEN", "HIGH", "LOW", "CLOSE",
            "VOLUME", "OI"
        }
        if not required.issubset(df.columns):
            continue

        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
        df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"])

        if last_date is not None:
            df = df[df["TRADE_DATE"] > last_date]

        if not df.empty:
            df = sanitize_futures_numeric(df)
            new_rows.append(df)

    if not new_rows:
        print("No new futures data to append")
        return

    daily_new = pd.concat(new_rows, ignore_index=True)
    print(f"New rows appended : {len(daily_new):,}")

    # --------------------------------------------------
    # Merge & dedupe
    # --------------------------------------------------
    combined = (
        pd.concat([master, daily_new], ignore_index=True)
        .drop_duplicates(
            subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"],
            keep="last"
        )
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .reset_index(drop=True)
    )

    # --------------------------------------------------
    # Backup before save
    # --------------------------------------------------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(
        MASTER_PQ,
        MASTER_PQ.with_name(f"master_futures_backup_{ts}.parquet")
    )
    shutil.copy2(
        MASTER_CSV,
        MASTER_CSV.with_name(f"master_futures_backup_{ts}.csv")
    )

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER FUTURES UPDATED SAFELY")
    print(f"Rows : {len(combined):,}")
    print(f"From : {combined['TRADE_DATE'].min().date()}")
    print(f"To   : {combined['TRADE_DATE'].max().date()}")
    print("DONE")

# ==================================================
if __name__ == "__main__":
    main()
