#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
from configs.paths import (
    RAW_EQUITY_DIR, RAW_FUTURES_DIR, RAW_OPTIONS_DIR
)

def describe_latest_csv(folder: Path, label: str):
    files = sorted(folder.glob("*.csv"))
    if not files:
        print(f"⚠️ No CSV files in {folder}")
        return

    latest = files[-1]
    print(f"\n[{label}] Latest CSV → {latest.name}")
    df = pd.read_csv(latest)
    print(f"   Rows : {len(df):,}")
    print(f"   Cols : {len(df.columns)}")
    print(f"   Columns: {list(df.columns)[:10]}{'...' if len(df.columns) > 10 else ''}")

def describe_latest_zip(folder: Path, label: str):
    files = sorted(folder.glob("*.zip"))
    if not files:
        print(f"⚠️ No ZIP files in {folder}")
        return

    latest = files[-1]
    print(f"\n[{label}] Latest ZIP → {latest.name}")
    print(f"   (ZIP content not inspected here; checked at cleaning stage)")

def main():
    print("🚀 NIFTY-LAB | RAW DATA SANITY CHECK")
    print("=" * 80)

    describe_latest_csv(RAW_EQUITY_DIR, "EQUITY")
    describe_latest_zip(RAW_FUTURES_DIR, "FUTURES")
    describe_latest_zip(RAW_OPTIONS_DIR, "OPTIONS")

    print("\n🎉 RAW SANITY CHECK COMPLETE ✅")

if __name__ == "__main__":
    main()
