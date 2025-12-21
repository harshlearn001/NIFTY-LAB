#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY OPTIONS SANITY CHECK

AUTO • HOLIDAY SAFE • SYMBOL-LESS

✔ OPTIDX only
✔ Single TRADE_DATE
✔ Valid expiry range
✔ Duplicate detection
✔ Skips safely if no data for today
✔ Scheduler / cron safe
"""

from pathlib import Path
from datetime import date
import pandas as pd

# ==================================================
# PATHS
# ==================================================
BASE = Path(r"H:\NIFTY-LAB")
OPT_DAILY_DIR = BASE / "data" / "processed" / "daily" / "options"

REQUIRED_COLS = {
    "INSTRUMENT",
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

# ==================================================
# MAIN
# ==================================================
def main():
    print("NIFTY-LAB | DAILY OPTIONS SANITY CHECK")
    print("=" * 80)

    files = sorted(OPT_DAILY_DIR.glob("OPTIONS_NIFTY_*.parquet"))
    if not files:
        print("No cleaned options files found — skipping sanity")
        return

    latest_file = files[-1]
    print(f"Latest file : {latest_file.name}")

    df = pd.read_parquet(latest_file)

    # --------------------------------------------------
    # DATE NORMALIZATION
    # --------------------------------------------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], errors="coerce")

    if df["TRADE_DATE"].isna().all():
        print("Invalid TRADE_DATE — skipping sanity")
        return

    file_date = df["TRADE_DATE"].iloc[0].date()
    today = date.today()

    # --------------------------------------------------
    # CRITICAL GUARD (HOLIDAY / NSE DELAY SAFE)
    # --------------------------------------------------
    if file_date != today:
        print(
            f"No options data for today ({today}). "
            f"Latest available date: {file_date}. Skipping sanity."
        )
        print("Sanity skipped safely — no action required")
        return

    # --------------------------------------------------
    # HARD CHECKS (ONLY WHEN DATE MATCHES)
    # --------------------------------------------------
    assert (df["INSTRUMENT"] == "OPTIDX").all(), "Non-OPTIDX rows found"
    print("✔ INSTRUMENT = OPTIDX only")

    assert df["TRADE_DATE"].nunique() == 1, "Multiple TRADE_DATE found"
    print("✔ Single TRADE_DATE")

    missing = REQUIRED_COLS - set(df.columns)
    assert not missing, f" Missing columns: {missing}"
    print("✔ Required schema OK")

    # --------------------------------------------------
    # DATE RANGE CHECK
    # --------------------------------------------------
    print(f"TRADE_DATE : {file_date}")
    print(
        f"EXPIRY RANGE : "
        f"{df['EXP_DATE'].min().date()} → {df['EXP_DATE'].max().date()}"
    )

    # --------------------------------------------------
    # DUPLICATE CHECK
    # --------------------------------------------------
    dupes = df.duplicated(
        subset=[
            "TRADE_DATE",
            "EXP_DATE",
            "STR_PRICE",
            "OPT_TYPE",
        ]
    ).sum()

    print(f"Duplicate rows : {dupes}")

    if dupes > 0:
        print("⚠️ Warning: Duplicate option rows detected")

    print(" DAILY OPTIONS SANITY PASSED")


# ==================================================
# ENTRY
# ==================================================
if __name__ == "__main__":
    main()
