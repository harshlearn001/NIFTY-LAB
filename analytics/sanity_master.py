#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
MASTER SANITY CHECK
Single source of truth: data/continuous/
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")
MASTER_DIR = BASE / "data" / "continuous"


def check_master(name, date_col):
    print("\n" + "=" * 80)
    print(f"📦 MASTER CHECK | {name.upper()}")
    print("=" * 80)

    file = MASTER_DIR / f"master_{name}.parquet"

    if not file.exists():
        print(f"⚠️ File not found → {file}")
        return

    df = pd.read_parquet(file)

    print(f"📄 File     : {file.name}")
    print(f"📊 Rows     : {len(df):,}")
    print(f"📐 Columns  : {len(df.columns)}")
    print(list(df.columns))

    # ---------------- DATE RANGE ----------------
    if date_col in df.columns:
        print("\n📅 DATE RANGE")
        print(f"From : {df[date_col].min()}")
        print(f"To   : {df[date_col].max()}")

    # ---------------- DUPLICATES ----------------
    dups = df.duplicated().sum()
    print(f"\n🔁 Duplicate rows : {dups}")

    # ---------------- MISSING VALUES ----------------
    na = df.isna().sum()
    na = na[na > 0]

    print("\n📉 Missing values (non-zero only)")
    print(na if not na.empty else "✅ No missing values")

    # ---------------- DTYPES ----------------
    print("\n📐 Data types")
    print(df.dtypes)

    # ---------------- EXTRA INFO ----------------
    if "SYMBOL" in df.columns:
        print("\n📊 SYMBOL DISTRIBUTION (Top 10)")
        print(df["SYMBOL"].value_counts().head(10))


def main():
    print("🚀 NIFTY-LAB | MASTER SANITY CHECK")

    check_master("equity",  "DATE")
    check_master("futures", "TRADE_DATE")
    check_master("options", "TRADE_DATE")

    print("\n🎉 MASTER SANITY CHECK COMPLETE ✅")


if __name__ == "__main__":
    main()
