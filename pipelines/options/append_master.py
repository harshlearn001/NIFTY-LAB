#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | APPEND DAILY NIFTY OPTIONS → MASTER

INPUT  : data/processed/daily/options/OPTIONS_NIFTY_YYYY-MM-DD.parquet
MASTER : data/continuous/master_options.parquet

✅ Uses ONLY NIFTY + OPTIDX (already ensured by cleaner)
✅ Fixes date formats (dd-mm-YYYY → datetime64)
✅ Works even if master was created earlier with strings
✅ Idempotent (safe to run multiple times)
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR   = BASE / "data" / "processed" / "daily" / "options"
CONTINUOUS  = BASE / "data" / "continuous"
CONTINUOUS.mkdir(parents=True, exist_ok=True)

MASTER_PQ   = CONTINUOUS / "master_options.parquet"
MASTER_CSV  = CONTINUOUS / "master_options.csv"

# Expected final schema
COLUMNS = [
    "INSTRUMENT",
    "SYMBOL",
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
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace("*", "", regex=False)
    )
    return df


def fix_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure both EXP_DATE and TRADE_DATE are proper datetime64[ns]
    regardless of whether they were strings, ints, or already datetimes.
    """
    # TRADE_DATE: can be dd-mm-YYYY string OR ns-int OR datetime
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")

    # EXP_DATE: exchange usually gives dd-mm-YYYY
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True, errors="coerce")

    return df


def ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
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
    print("🚀 NIFTY-LAB | APPEND DAILY NIFTY OPTIONS → MASTER")
    print("-" * 60)

    daily_files = sorted(DAILY_DIR.glob("OPTIONS_NIFTY_*.parquet"))
    if not daily_files:
        print("⚠️ No daily NIFTY options files found")
        return

    print(f"📄 Daily files found: {len(daily_files)}")

    # ---------- load existing master (if any) ----------
    if MASTER_PQ.exists():
        master = pd.read_parquet(MASTER_PQ)
        master = normalize_columns(master)
        master = fix_dates(master)
        master = ensure_numeric(master)
        master = master[COLUMNS]
        before_rows = len(master)
        print(f"✅ Loaded existing master: {before_rows:,} rows")
    else:
        master = pd.DataFrame(columns=COLUMNS)
        before_rows = 0
        print("ℹ️ Master does not exist yet → will create new one")

    # ---------- load all daily files ----------
    daily_frames = []
    for f in daily_files:
        df = pd.read_parquet(f)
        df = normalize_columns(df)
        df = fix_dates(df)
        df = ensure_numeric(df)

        # make sure all expected columns exist
        missing = [c for c in COLUMNS if c not in df.columns]
        if missing:
            print(f"⚠️ {f.name} skipped (missing columns: {missing})")
            continue

        df = df[COLUMNS]

        # sanity: only OPTIDX / NIFTY (cleaner should guarantee this)
        df = df[(df["INSTRUMENT"] == "OPTIDX") & (df["SYMBOL"] == "NIFTY")]
        if df.empty:
            print(f"⚠️ {f.name} has no NIFTY OPTIDX rows (skipped)")
            continue

        daily_frames.append(df)

    if not daily_frames:
        print("⚠️ No valid daily NIFTY options rows to append")
        return

    daily_all = pd.concat(daily_frames, ignore_index=True)

    # ---------- combine with master ----------
    combined = (
        pd.concat([master, daily_all], ignore_index=True)
        .drop_duplicates(
            subset=[
                "INSTRUMENT",
                "SYMBOL",
                "TRADE_DATE",
                "EXP_DATE",
                "STR_PRICE",
                "OPT_TYPE",
            ]
        )
        .sort_values(["TRADE_DATE", "EXP_DATE", "STR_PRICE", "OPT_TYPE"])
        .reset_index(drop=True)
    )

    after_rows = len(combined)
    added = after_rows - before_rows

    # ---------- save ----------
    combined.to_parquet(MASTER_PQ, index=False)
    combined.to_csv(MASTER_CSV, index=False)

    print("-" * 60)
    print("✅ MASTER OPTIONS UPDATED")
    print(f"📊 Rows : {after_rows:,}")
    print(f"➕ New rows added: {max(added, 0):,}")
    if not combined["TRADE_DATE"].isna().all():
        print(f"📅 From : {combined['TRADE_DATE'].min().date()}")
        print(f"📅 To   : {combined['TRADE_DATE'].max().date()}")
    print(f"💾 Parquet : {MASTER_PQ}")
    print("🎉 DONE ✅")


if __name__ == "__main__":
    main()
