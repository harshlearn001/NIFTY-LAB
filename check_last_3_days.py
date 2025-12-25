#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | LAST 3 DAYS DATA CHECK (CONTINUOUS)

Checks:
✔ master_equity
✔ master_futures
✔ master_options

Shows:
- Last 3 dates
- Rows per date
- Volume / OI sanity
"""

import pandas as pd
from pathlib import Path

BASE = Path(r"H:\NIFTY-LAB\data\continuous")

FILES = {
    "EQUITY": BASE / "master_equity.parquet",
    "FUTURES": BASE / "master_futures.parquet",
    "OPTIONS": BASE / "master_options.parquet",
}

# ----------------------------------------
def detect_date_col(df):
    for c in ["DATE", "TRADE_DATE", "date"]:
        if c in df.columns:
            return c
    raise ValueError("❌ No date column found")

# ----------------------------------------
def check_file(label, path):
    print(f"\n{'='*80}")
    print(f"📊 {label}")
    print(f"📂 File : {path.name}")

    df = pd.read_parquet(path)
    date_col = detect_date_col(df)

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values(date_col)

    last_dates = df[date_col].drop_duplicates().tail(3)

    print("\n📅 Last 3 available dates:")
    for d in last_dates:
        rows = df[df[date_col] == d]
        print(
            f"  {d.date()} | rows={len(rows):,}",
            end=""
        )

        # Optional sanity hints
        if "VOLUME" in rows.columns:
            print(f" | volume_sum={rows['VOLUME'].sum():,}", end="")
        if "OI" in rows.columns:
            print(f" | oi_sum={rows['OI'].sum():,}", end="")

        print()

# ----------------------------------------
def main():
    print("\n🚀 NIFTY-LAB | LAST 3 DAYS MASTER DATA CHECK")

    for label, path in FILES.items():
        if not path.exists():
            print(f"\n❌ Missing file: {path}")
            continue
        check_file(label, path)

    print("\n✅ CHECK COMPLETE")

# ----------------------------------------
if __name__ == "__main__":
    main()
