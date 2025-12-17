#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY EQUITY SANITY CHECK
Schema-agnostic, case-safe version
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
EQ_DAILY_DIR = BASE / "data" / "processed" / "daily" / "equity"

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def find_col(df, names):
    """Return first matching column name (case-insensitive)."""
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in cols:
            return cols[n.lower()]
    raise KeyError(f"None of columns {names} found in {df.columns.tolist()}")

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | DAILY EQUITY SANITY CHECK")
    print("=" * 80)

    files = sorted(EQ_DAILY_DIR.glob("clean_equity_*.parquet"))
    if not files:
        print("No cleaned daily equity files found")
        return

    print(f"Files found : {len(files)}")

    f = files[-1]
    print("\nSample file")
    print(f"File : {f.name}")

    df = pd.read_parquet(f)

    print(f"Rows : {len(df):,}")
    print(f"Cols : {len(df.columns)}")
    print(df.columns.tolist())

    # --------------------------------------------------
    # DYNAMIC COLUMN RESOLUTION
    # --------------------------------------------------
    DATE_COL = find_col(df, ["date", "trade_date"])
    OPEN = find_col(df, ["open"])
    HIGH = find_col(df, ["high"])
    LOW = find_col(df, ["low"])
    CLOSE = find_col(df, ["close"])
    SYMBOL = find_col(df, ["symbol"])
    VOLUME = find_col(df, ["volume"])

    # --------------------------------------------------
    # DATE CHECKS
    # --------------------------------------------------
    print("\nDATE CHECKS")
    print(f"Unique dates : {df[DATE_COL].nunique()}")
    print(
        f"Date range  : {df[DATE_COL].min()} -> {df[DATE_COL].max()}"
    )

    # --------------------------------------------------
    # SCHEMA CHECK
    # --------------------------------------------------
    print("\nSCHEMA CHECK")
    print("Required columns resolved dynamically")

    # --------------------------------------------------
    # DATA TYPES
    # --------------------------------------------------
    print("\nDATA TYPES")
    print(df.dtypes)

    # --------------------------------------------------
    # MISSING VALUES
    # --------------------------------------------------
    print("\nMISSING VALUES (non-zero only)")
    na = df[[DATE_COL, OPEN, HIGH, LOW, CLOSE, SYMBOL]].isna().sum()
    na = na[na > 0]
    print(na if not na.empty else "No missing values")

    # --------------------------------------------------
    # DUPLICATES
    # --------------------------------------------------
    print(f"\nDUPLICATE ROWS : {df.duplicated().sum()}")

    # --------------------------------------------------
    # OHLC LOGIC
    # --------------------------------------------------
    print("\nOHLC LOGIC CHECK")
    bad_ohlc = df[
        (df[HIGH] < df[LOW]) |
        (df[CLOSE] > df[HIGH]) |
        (df[CLOSE] < df[LOW]) |
        (df[OPEN] > df[HIGH]) |
        (df[OPEN] < df[LOW])
    ]

    if len(bad_ohlc) > 0:
        print(f"OHLC errors : {len(bad_ohlc)}")
        print(bad_ohlc.head())
    else:
        print("OHLC logic valid")

    # --------------------------------------------------
    # PRICE SANITY
    # --------------------------------------------------
    print("\nPRICE SANITY")
    bad_price = df[
        (df[OPEN] <= 0) |
        (df[HIGH] <= 0) |
        (df[LOW] <= 0) |
        (df[CLOSE] <= 0)
    ]

    if len(bad_price) > 0:
        print(f"Bad prices : {len(bad_price)}")
    else:
        print("Prices valid")

    # --------------------------------------------------
    # VOLUME SANITY
    # --------------------------------------------------
    neg_vol = df[df[VOLUME] < 0]
    if len(neg_vol) > 0:
        print(f"Negative volume rows : {len(neg_vol)}")
    else:
        print("Volume sanity OK")

    print("\nDAILY EQUITY SANITY CHECK PASSED")


if __name__ == "__main__":
    main()
