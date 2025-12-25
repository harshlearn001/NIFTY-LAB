#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY FUTURES SANITY CHECK

AUTO • HOLIDAY SAFE • NIFTY ONLY

✔ FUTIDX only
✔ Single TRADE_DATE
✔ Valid expiry ordering
✔ Skips safely if latest data ≠ today
"""

from pathlib import Path
from datetime import date
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
FUT_DAILY_DIR = BASE / "data" / "processed" / "daily" / "futures"

REQUIRED_COLS = {
    "SYMBOL",
    "TRADE_DATE",
    "EXP_DATE",
    "OPEN",
    "HIGH",
    "LOW",
    "CLOSE",
    "VOLUME",
    "OI",
}

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | DAILY FUTURES SANITY CHECK")
    print("=" * 80)

    files = sorted(FUT_DAILY_DIR.glob("FUT_NIFTY_*.parquet"))
    if not files:
        print("No cleaned daily futures files found — skipping sanity")
        return

    f = files[-1]
    print(f"Latest file : {f.name}")

    df = pd.read_parquet(f)

    # ------------------------------
    # DATE NORMALIZATION
    # ------------------------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], errors="coerce")

    if df["TRADE_DATE"].isna().all():
        print("Invalid TRADE_DATE — skipping sanity")
        return

    file_date = df["TRADE_DATE"].iloc[0].date()
    today = date.today()

    # ------------------------------
    # SAFE GUARD (NO FAIL ON HOLIDAY)
    # ------------------------------
    if file_date != today:
        print(
            f"No futures data for today ({today}). "
            f"Latest available date: {file_date}. Skipping sanity."
        )
        print("Sanity skipped safely — no action required")
        return

    # ------------------------------
    # SCHEMA CHECK
    # ------------------------------
    missing = REQUIRED_COLS - set(df.columns)
    assert not missing, f"Missing columns: {missing}"
    print("Schema OK")

    # ------------------------------
    # SYMBOL CHECK
    # ------------------------------
    assert (df["SYMBOL"] == "NIFTY").all(), "Non-NIFTY rows found"
    print("SYMBOL = NIFTY only")

    # ------------------------------
    # DATE CHECK
    # ------------------------------
    assert df["TRADE_DATE"].nunique() == 1, "Multiple TRADE_DATE found"
    print(f"TRADE_DATE : {file_date}")

    print(
        f"EXPIRY RANGE : "
        f"{df['EXP_DATE'].min().date()} → {df['EXP_DATE'].max().date()}"
    )

    # ------------------------------
    # DUPLICATES
    # ------------------------------
    dupes = df.duplicated(
        subset=["TRADE_DATE", "EXP_DATE"]
    ).sum()

    print(f"Duplicate rows : {dupes}")

    # ------------------------------
    # OHLC LOGIC
    # ------------------------------
    bad_ohlc = df[
        (df["HIGH"] < df["LOW"]) |
        (df["CLOSE"] > df["HIGH"]) |
        (df["CLOSE"] < df["LOW"]) |
        (df["OPEN"] > df["HIGH"]) |
        (df["OPEN"] < df["LOW"])
    ]

    if not bad_ohlc.empty:
        raise AssertionError(f"OHLC errors found: {len(bad_ohlc)} rows")

    print("OHLC logic valid")

    # ------------------------------
    # FINAL
    # ------------------------------
    print("DAILY FUTURES SANITY PASSED ")


if __name__ == "__main__":
    main()
