#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY PROCESSED DATA SANITY CHECK
"""

import sys
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------------
# ✅ FIX IMPORT PATH ONCE
# ------------------------------------------------------------------
BASE = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE))

from configs.paths import PROC_EQ_DAILY, PROC_FUT_DAILY, PROC_OPT_DAILY
from configs.schema import EQUITY_SCHEMA, OPTIONS_DAILY_SCHEMA


def sample_parquet(folder: Path, label: str, schema: dict | None = None):
    print(f"\n📦 {label}")
    print("-" * 60)
    print(f"📂 Folder : {folder}")

    files = sorted(folder.glob("*.parquet"))

    if not files:
        print("⚠️ No parquet files found")
        return

    f = files[0]
    df = pd.read_parquet(f)

    print(f"📄 Sample file : {f.name}")
    print(f"📊 Rows        : {len(df):,}")
    print(f"📐 Columns     : {len(df.columns)}")
    print(list(df.columns))

    if "TRADE_DATE" in df.columns:
        print(f"📅 TRADE_DATE unique : {df['TRADE_DATE'].nunique()}")

    if schema:
        missing = set(schema) - set(df.columns)
        extra   = set(df.columns) - set(schema)

        if missing:
            print(f"❌ Missing columns : {sorted(missing)}")
        if extra:
            print(f"ℹ️ Extra columns   : {sorted(extra)}")


def main():
    print("🚀 NIFTY-LAB | DAILY PROCESSED SANITY CHECK")
    print("=" * 80)

    sample_parquet(PROC_EQ_DAILY,  "EQUITY",  EQUITY_SCHEMA)
    sample_parquet(PROC_FUT_DAILY, "FUTURES")
    sample_parquet(PROC_OPT_DAILY, "OPTIONS", OPTIONS_DAILY_SCHEMA)

    print("\n🎉 DAILY SANITY CHECK COMPLETE ✅")


if __name__ == "__main__":
    main()
