#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY OPTIONS SANITY CHECK (NIFTY ONLY)

✔ OPTIDX only
✔ SYMBOL = NIFTY only
✔ Handles NSE nanosecond dates
✔ Append-safe
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
OPT_DAILY_DIR = BASE / "data" / "processed" / "daily" / "options"

REQUIRED_COLS = {
    "INSTRUMENT",
    "SYMBOL",
    "TRADE_DATE",
    "EXP_DATE",
    "STR_PRICE",
    "OPT_TYPE",
    "OPEN_PRICE",
    "HI_PRICE",
    "LO_PRICE",
    "CLOSE_PRICE",
    "OPEN_INT",
}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def fix_ns_date(s):
    """
    Convert NSE nanoseconds / strings → datetime
    """
    return pd.to_datetime(s, errors="coerce")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("🚀 NIFTY-LAB | DAILY OPTIONS SANITY CHECK (NIFTY ONLY)")
    print("=" * 80)

    files = sorted(OPT_DAILY_DIR.glob("OPTIONS_NIFTY_*.parquet"))
    if not files:
        print("⚠️ No cleaned NIFTY options files found")
        return

    f = files[-1]
    print(f"\n📄 File : {f.name}")

    df = pd.read_parquet(f)

    # ---- FIX DATES FOR SANITY ONLY ----
    df["TRADE_DATE"] = fix_ns_date(df["TRADE_DATE"])
    df["EXP_DATE"]   = fix_ns_date(df["EXP_DATE"])

    # --------------------------------------------------
    # BASIC INFO
    # --------------------------------------------------
    print("\n📊 BASIC INFO")
    print(f"Rows    : {len(df):,}")
    print(f"Columns : {len(df.columns)}")
    print(df.columns.tolist())

    # --------------------------------------------------
    # HARD CHECKS
    # --------------------------------------------------
    print("\n✅ HARD CHECKS")

    assert (df["INSTRUMENT"] == "OPTIDX").all(), "❌ Non-OPTIDX rows found"
    print("✔ INSTRUMENT = OPTIDX only")

    assert (df["SYMBOL"] == "NIFTY").all(), "❌ Non-NIFTY rows found"
    print("✔ SYMBOL = NIFTY only")

    assert df["TRADE_DATE"].nunique() == 1, "❌ Multiple TRADE_DATE found"
    print("✔ Single TRADE_DATE")

    missing = REQUIRED_COLS - set(df.columns)
    assert not missing, f"❌ Missing columns: {missing}"
    print("✔ Required schema OK")

    # --------------------------------------------------
    # DATE CHECKS
    # --------------------------------------------------
    print("\n📅 DATE CHECKS")
    print(f"TRADE_DATE : {df['TRADE_DATE'].iloc[0].date()}")
    print(
        f"EXPIRY RANGE : {df['EXP_DATE'].min().date()} → {df['EXP_DATE'].max().date()}"
    )

    # --------------------------------------------------
    # MISSING VALUES
    # --------------------------------------------------
    print("\n📉 MISSING VALUES (non-zero only)")
    na = df.isna().sum()
    na = na[na > 0]
    print(na if not na.empty else "✅ No missing values")

    # --------------------------------------------------
    # DUPLICATES
    # --------------------------------------------------
    dupes = df.duplicated(
        subset=[
            "SYMBOL",
            "TRADE_DATE",
            "EXP_DATE",
            "STR_PRICE",
            "OPT_TYPE",
        ]
    ).sum()
    print(f"\n🔁 DUPLICATE CONTRACT ROWS : {dupes}")

    # --------------------------------------------------
    # DISTRIBUTION
    # --------------------------------------------------
    print("\n📊 OPTION TYPE DISTRIBUTION")
    print(df["OPT_TYPE"].value_counts())

    print("\n🎉 DAILY NIFTY OPTIONS SANITY PASSED ✅")


if __name__ == "__main__":
    main()
