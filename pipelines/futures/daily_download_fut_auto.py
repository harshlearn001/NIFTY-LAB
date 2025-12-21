#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY FUTURES DOWNLOAD (FO ZIP)
AUTO MODE — SAFE FOR SCHEDULER

✔ Looks back up to 7 days
✔ Weekend-aware
✔ Logs when file already exists
✔ NEVER breaks pipeline
✔ RAW ONLY — no cleaning here

Downloads:
  foDDMMYYYY.zip from NSE

Saves:
  data/raw/futures/fo_YYYY-MM-DD.zip
"""

import argparse
import requests
from datetime import datetime, timedelta
from pathlib import Path

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
OUT_DIR = BASE / "data" / "raw" / "futures"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# NSE CONFIG
# --------------------------------------------------
BASE_URL = "https://nsearchives.nseindia.com/archives/fo/mkt/fo{date}.zip"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
}

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main(trade_date):
    print("NIFTY-LAB | DAILY FUTURES DOWNLOAD (AUTO)")
    print("-" * 60)

    for i in range(7):  # look back up to 7 days
        d = trade_date - timedelta(days=i)

        print(f"Trying FO bhavcopy for {d}")

        # Weekend skip
        if d.weekday() >= 5:
            print("   Weekend — skipped")
            continue

        out_file = OUT_DIR / f"fo_{d:%Y-%m-%d}.zip"

        # IMPORTANT: explicit log if already exists
        if out_file.exists():
            print(f" Already exists → {out_file.name}")
            return  # SOFT EXIT (scheduler-safe)

        d_nse = d.strftime("%d%m%Y")
        url = BASE_URL.format(date=d_nse)

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)

            if r.status_code == 200 and len(r.content) > 50_000:
                out_file.write_bytes(r.content)
                print(f" Downloaded & saved → {out_file.name}")
                return
            else:
                print("   Not available")

        except requests.RequestException as e:
            print(f" Network error : {e}")

    #  NEVER FAIL PIPELINE
    print("No FO bhavcopy found in recent days — skipping safely")
    return


# --------------------------------------------------
# CLI
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download daily NSE FO futures bhavcopy"
    )
    parser.add_argument(
        "--date",
        help="Trade date YYYY-MM-DD (default: today)",
        required=False,
    )

    args = parser.parse_args()

    trade_date = (
        datetime.strptime(args.date, "%Y-%m-%d").date()
        if args.date
        else datetime.today().date()
    )

    main(trade_date)
