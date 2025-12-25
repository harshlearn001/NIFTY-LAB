#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY NIFTY OPTIONS TO MASTER (LOCKED)

✔ Append-only
✔ Date-aware
✔ MASTER SAFETY LOCK
✔ Auto-backup before write
✔ NIFTY OPTIDX only
✔ Deduplicated & sorted
✔ Scheduler-safe
"""

from pathlib import Path
import pandas as pd
import shutil
from datetime import datetime

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR  = BASE / "data" / "processed" / "daily" / "options"
CONTINUOUS = BASE / "data" / "continuous"
CONTINUOUS.mkdir(parents=True, exist_ok=True)

MASTER_PQ  = CONTINUOUS / "master_options.parquet"
MASTER_CSV = CONTINUOUS / "master_options.csv"

# --------------------------------------------------
# FINAL MASTER SCHEMA
# --------------------------------------------------
COLUMNS = [
    "INSTRUMENT",
    "EXP_DATE",
    "STR_PRICE",
    "OPT_TYPE",
    "OPEN_PRICE",
    "HI_PRICE",
    "LO_PRICE",
    "CLOSE_PRICE",
    "OPEN_INT",
    "TRD_QTY",
    "NO_OF_CONT",
    "NO_OF_TRADE",
    "NOTION_VAL",
    "PR_VAL",
    "TRADE_DATE",
]

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def normalize_columns(df):
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace("*", "", regex=False)
    )
    return df


def fix_dates(df):
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"], errors="coerce")
    return df


def ensure_numeric(df):
    numeric_cols = [
        "STR_PRICE",
        "OPEN_PRICE",
        "HI_PRICE",
        "LO_PRICE",
        "CLOSE_PRICE",
        "OPEN_INT",
        "TRD_QTY",
        "NO_OF_CONT",
        "NO_OF_TRADE",
        "NOTION_VAL",
        "PR_VAL",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | APPEND DAILY NIFTY OPTIONS TO MASTER")
    print("-" * 60)

    daily_files = sorted(DAILY_DIR.glob("OPTIONS_NIFTY_*.parquet"))
    if not daily_files:
        print("No daily NIFTY options files found")
        return

    print(f"Daily files found : {len(daily_files)}")

    # ---------- Load master with HARD LOCK ----------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master = normalize_columns(master)
        master = fix_dates(master)
        master = ensure_numeric(master)
        master = master[COLUMNS]

        print(f"Loaded master rows : {len(master):,}")

        # 🔒 SAFETY LOCK
        if len(master) < 100_000:
            raise RuntimeError(
                "MASTER OPTIONS TOO SMALL — POSSIBLE CORRUPTION. "
                "REFUSING TO MODIFY."
            )

        last_date = master["TRADE_DATE"].max()
        print(f"Last master date : {last_date.date()}")

    else:
        master = pd.DataFrame(columns=COLUMNS)
        last_date = None
        print("Master does not exist — creating new")

    # ---------- Load only NEW daily data ----------
    daily_frames = []

    for f in daily_files:
        df = pd.read_parquet(f)
        df = normalize_columns(df)
        df = fix_dates(df)
        df = ensure_numeric(df)

        if last_date is not None and df["TRADE_DATE"].max() <= last_date:
            continue

        missing = [c for c in COLUMNS if c not in df.columns]
        if missing:
            continue

        df = df[COLUMNS]

        # OPTIDX only
        df = df[df["INSTRUMENT"] == "OPTIDX"]

        if last_date is not None:
            df = df[df["TRADE_DATE"] > last_date]

        if not df.empty:
            daily_frames.append(df)

    if not daily_frames:
        print("No new options data to append")
        return

    daily_new = pd.concat(daily_frames, ignore_index=True)
    print(f"New rows appended : {len(daily_new):,}")

    # ---------- Combine & dedupe ----------
    combined = (
        pd.concat([master, daily_new], ignore_index=True)
        .drop_duplicates(
            subset=[
                "INSTRUMENT",
                "TRADE_DATE",
                "EXP_DATE",
                "STR_PRICE",
                "OPT_TYPE",
            ],
            keep="last",
        )
        .sort_values(
            ["TRADE_DATE", "EXP_DATE", "STR_PRICE", "OPT_TYPE"]
        )
        .reset_index(drop=True)
    )

    # ---------- Backup before save ----------
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(
        MASTER_PQ,
        MASTER_PQ.with_name(f"master_options_backup_{ts}.parquet")
    )
    shutil.copy2(
        MASTER_CSV,
        MASTER_CSV.with_name(f"master_options_backup_{ts}.csv")
    )

    # ---------- Save ----------
    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("MASTER OPTIONS UPDATED SAFELY")
    print(f"Rows : {len(combined):,}")
    print(f"From : {combined['TRADE_DATE'].min().date()}")
    print(f"To   : {combined['TRADE_DATE'].max().date()}")
    print("DONE")


if __name__ == "__main__":
    main()
