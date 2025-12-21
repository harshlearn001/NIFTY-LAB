#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY EQUITY (AUTO | NIFTY INDEX)

✔ Auto-picks latest raw equity file
✔ Safe on holidays / NSE delays
✔ Parquet + CSV
✔ NEVER breaks scheduler
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
RAW_DIR = BASE / "data" / "raw" / "equity"
OUT_DIR = BASE / "data" / "processed" / "daily" / "equity"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def find_latest_raw(max_lookback=7):
    today = datetime.today().date()
    for i in range(max_lookback):
        d = today - timedelta(days=i)
        f = RAW_DIR / f"equity_{d}.csv"
        if f.exists():
            return d, f
    return None, None

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | CLEAN DAILY EQUITY (AUTO)")
    print("-" * 60)

    trade_date, raw_file = find_latest_raw()

    if raw_file is None:
        print("No raw equity file found in recent days — skipping clean")
        return  #  SOFT EXIT

    out_pq = OUT_DIR / f"EQUITY_NIFTY_{trade_date}.parquet"
    out_csv = OUT_DIR / f"EQUITY_NIFTY_{trade_date}.csv"

    if out_pq.exists():
        print(f"Already cleaned → {out_pq.name}")
        return  #  SOFT EXIT

    print(f"Using raw file : {raw_file.name}")
    print(f"Trade Date    : {trade_date}")

    df = pd.read_csv(raw_file)

    df.columns = df.columns.str.upper().str.strip()

    df = df[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "SYMBOL"]]

    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y-%m-%d")

    df["SYMBOL"] = "NIFTY"

    for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = (
        df.dropna()
          .sort_values("DATE")
          .drop_duplicates(subset=["DATE", "SYMBOL"])
    )

    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)

    print(f"Saved : {out_pq.name}")
    print(f"Rows  : {len(df)}")
    print(" DAILY EQUITY CLEAN COMPLETE")

# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Non-fatal error: {e}")
        exit(0)  #  NEVER FAIL PIPELINE
