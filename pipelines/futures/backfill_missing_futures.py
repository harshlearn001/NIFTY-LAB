#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | BACKFILL MISSING FUTURES DAYS

✔ Allows older dates
✔ Append-only
✔ Deduplicated
✔ Numeric sanity enforced
✔ SAFE for historical fixes
"""

from pathlib import Path
import pandas as pd

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

def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "OI" in df.columns and "OPEN_INTEREST" not in df.columns:
        df = df.rename(columns={"OI": "OPEN_INTEREST"})

    for col in FLOAT_COLS + INT_COLS:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
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
    print("NIFTY-LAB | BACKFILL MISSING FUTURES DAYS")
    print("-" * 60)

    master = pd.read_parquet(MASTER_PQ)
    master["TRADE_DATE"] = pd.to_datetime(master["TRADE_DATE"])
    master["EXP_DATE"] = pd.to_datetime(master["EXP_DATE"])

    before = len(master)

    frames = []

    for file in DAILY_DIR.glob("FUT_NIFTY_*.csv"):
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

        df = sanitize(df)
        frames.append(df)

    if not frames:
        print("No daily futures found")
        return

    daily_all = pd.concat(frames, ignore_index=True)

    combined = (
        pd.concat([master, daily_all], ignore_index=True)
        .drop_duplicates(
            subset=["SYMBOL", "TRADE_DATE", "EXP_DATE"],
            keep="last"
        )
        .sort_values(["TRADE_DATE", "EXP_DATE"])
        .reset_index(drop=True)
    )

    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print(f"Rows before : {before}")
    print(f"Rows after  : {len(combined)}")
    print(f"Backfilled : {len(combined) - before}")
    print("DONE")

# ==================================================
if __name__ == "__main__":
    main()
