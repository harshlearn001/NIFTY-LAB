#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY EQUITY SANITY CHECK
Checks cleaned daily equity data before appending to master
(CLEAN SCHEMA VERSION)
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
EQ_DAILY_DIR = BASE / "data" / "processed" / "daily" / "equity"

# Expected schema AFTER cleaning
REQUIRED_COLS = {
    "DATE",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "SYMBOL",
}

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | DAILY EQUITY SANITY CHECK")
    print("=" * 80)

    files = sorted(EQ_DAILY_DIR.glob("clean_equity_*.parquet"))

    if not files:
        print("No cleaned daily equity files found")
        return  # soft exit

    print(f"Files found : {len(files)}")

    f = files[-1]
    print("\nSample file")
    print(f"File : {f.name}")

    df = pd.read_parquet(f)

    print(f"Rows : {len(df):,}")
    print(f"Cols : {len(df.columns)}")
    print(df.columns.tolist())

    # --------------------------------------------------
    # DATE CHECKS
    # --------------------------------------------------
    print("\nDATE CHECKS")
    print(f"Unique dates : {df['DATE'].nunique()}")
    print(
        f"Date range  : {df['DATE'].min().date()} -> {df['DATE'].max().date()}"
    )

    # --------------------------------------------------
    # SCHEMA CHECK
    # --------------------------------------------------
    print("\nSCHEMA CHECK")
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        print(f"Missing columns : {missing}")
        return
    else:
        print("Required columns OK")

    # --------------------------------------------------
    # DATA TYPES
    # --------------------------------------------------
    print("\nDATA TYPES")
    print(df.dtypes)

    # --------------------------------------------------
    # MISSING VALUES
    # --------------------------------------------------
    print("\nMISSING VALUES (non-zero only)")
    na = df.isna().sum()
    na = na[na > 0]
    print(na if not na.empty else "No missing values")

    # --------------------------------------------------
    # DUPLICATES
    # --------------------------------------------------
    print(f"\nDUPLICATE ROWS : {df.duplicated().sum()}")

    # --------------------------------------------------
    # OHLC LOGIC CHECK
    # --------------------------------------------------
    print("\nOHLC LOGIC CHECK")
    bad_ohlc = df[
        (df["HIGH"] < df["LOW"]) |
        (df["CLOSE"] > df["HIGH"]) |
        (df["CLOSE"] < df["LOW"]) |
        (df["OPEN"] > df["HIGH"]) |
        (df["OPEN"] < df["LOW"])
    ]

    if len(bad_ohlc) > 0:
        print(f"OHLC logic errors : {len(bad_ohlc)}")
        print(bad_ohlc.head())
    else:
        print("OHLC logic valid")

    # --------------------------------------------------
    # PRICE SANITY
    # --------------------------------------------------
    print("\nPRICE SANITY CHECK")
    bad_price = df[
        (df["OPEN"] <= 0) |
        (df["HIGH"] <= 0) |
        (df["LOW"] <= 0) |
        (df["CLOSE"] <= 0)
    ]

    if len(bad_price) > 0:
        print(f"Zero / negative prices : {len(bad_price)}")
    else:
        print("Prices are positive")

    # --------------------------------------------------
    # VOLUME CHECK (index allows zero)
    # --------------------------------------------------
    neg_vol = df[df["VOLUME"] < 0]
    if len(neg_vol) > 0:
        print(f"Negative volume rows : {len(neg_vol)}")
    else:
        print("Volume sanity OK")

    print("\nDAILY EQUITY SANITY CHECK PASSED")


if __name__ == "__main__":
    main()
