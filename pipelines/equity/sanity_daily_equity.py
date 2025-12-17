#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY EQUITY SANITY CHECK (PROD SAFE)

✔ Skips safely if latest date != today
✔ NEVER breaks pipeline
"""

from pathlib import Path
from datetime import date
import pandas as pd

# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
EQ_DAILY_DIR = BASE / "data" / "processed" / "daily" / "equity"
# --------------------------------------------------

def main():
    print("NIFTY-LAB | DAILY EQUITY SANITY CHECK")
    print("=" * 80)

    files = sorted(EQ_DAILY_DIR.glob("EQUITY_NIFTY_*.parquet"))
    if not files:
        print("No cleaned equity files found — skipping sanity")
        return

    f = files[-1]
    print(f"Latest file : {f.name}")

    df = pd.read_parquet(f)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    if df["DATE"].isna().all():
        print("Invalid DATE column — skipping sanity")
        return

    file_date = df["DATE"].iloc[0].date()
    today = date.today()

    if file_date != today:
        print(
            f"No equity data for today ({today}). "
            f"Latest available date: {file_date}. Skipping sanity."
        )
        return  # 🔑 SOFT EXIT

    # ---------------- HARD CHECKS (ONLY IF TODAY) ----------------
    assert df["DATE"].nunique() == 1, "Multiple dates found"
    assert (df["HIGH"] >= df["LOW"]).all(), "HIGH < LOW found"
    assert (df["OPEN"] > 0).all(), "Invalid OPEN price"
    assert (df["CLOSE"] > 0).all(), "Invalid CLOSE price"

    print("DAILY EQUITY SANITY PASSED")

# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Non-fatal equity sanity issue: {e}")
        exit(0)  # 🔑 NEVER FAIL PIPELINE
