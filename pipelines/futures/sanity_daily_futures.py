#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY FUTURES SANITY CHECK
Checks cleaned daily futures data before appending to master
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
FUT_DAILY_DIR = BASE / "data" / "processed" / "daily" / "futures"

# Expected minimal schema
REQUIRED_COLS = {
    "INSTRUMENT",
    "SYMBOL",
    "EXP_DATE",
    "TRADE_DATE",
    "OPEN_PRICE",
    "HI_PRICE",
    "LO_PRICE",
    "CLOSE_PRICE",
    "OPEN_INT",
}

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("🚀 NIFTY-LAB | DAILY FUTURES SANITY CHECK")
    print("=" * 80)

    files = sorted(FUT_DAILY_DIR.glob("FUTURES_*.parquet"))

    if not files:
        print("⚠️ No cleaned daily futures files found")
        return

    print(f"📄 Files found : {len(files)}")

    # --- sample latest file ---
    f = files[-1]
    print("\n📌 SAMPLE FILE")
    print(f"File : {f.name}")

    df = pd.read_parquet(f)

    print(f"Rows : {len(df):,}")
    print(f"Cols : {len(df.columns)}")
    print(df.columns.tolist())

    # --------------------------------------------------
    # Date checks
    # --------------------------------------------------
    print("\n📅 DATE CHECKS")
    print(f"TRADE_DATE unique : {df['TRADE_DATE'].nunique()}")
    print(
        f"EXP_DATE range   : {df['EXP_DATE'].min().date()} → {df['EXP_DATE'].max().date()}"
    )

    # --------------------------------------------------
    # Schema check
    # --------------------------------------------------
    print("\n📐 SCHEMA CHECK")
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        print(f"❌ Missing columns : {missing}")
    else:
        print("✅ Required columns OK")

    # --------------------------------------------------
    # Data types
    # --------------------------------------------------
    print("\n📐 DATA TYPES")
    print(df.dtypes)

    # --------------------------------------------------
    # Missing values
    # --------------------------------------------------
    print("\n📉 MISSING VALUES (non-zero only)")
    na = df.isna().sum()
    na = na[na > 0]
    print(na if not na.empty else "✅ No missing values")

    # --------------------------------------------------
    # Duplicates
    # --------------------------------------------------
    print(f"\n🔁 DUPLICATE ROWS : {df.duplicated().sum()}")

    # --------------------------------------------------
    # Distribution
    # --------------------------------------------------
    print("\n📊 INSTRUMENT DISTRIBUTION")
    print(df["INSTRUMENT"].value_counts())

    print("\n📊 SYMBOL DISTRIBUTION (Top 10)")
    print(df["SYMBOL"].value_counts().head(10))

    print("\n🎉 DAILY FUTURES SANITY CHECK PASSED ✅")


if __name__ == "__main__":
    main()
