#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY OPTIONS DOWNLOAD (FO ZIP)
AUTO MODE — SAFE FOR SCHEDULER

Downloads:
  foDDMMYYYY.zip from NSE

Saves:
  data/raw/options/fo_YYYY-MM-DD.zip

RAW DATA ONLY — DO NOT CLEAN HERE
"""

import argparse
import requests
from datetime import datetime
from pathlib import Path

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

OUT_DIR = BASE / "data" / "raw" / "options"
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
from datetime import timedelta

def main(trade_date):
    print("NIFTY-LAB | DAILY OPTIONS DOWNLOAD (AUTO)")
    print("-" * 60)

    for i in range(0, 7):  # look back up to 7 days
        d = trade_date - timedelta(days=i)

        print(f"📅 Trying FO bhavcopy for {d}")

        if d.weekday() >= 5:
            print("  ⏭ Weekend — skipped")
            continue

        out = OUT_DIR / f"fo_{d:%Y-%m-%d}.zip"
        if out.exists():
            print(f"  ✅ Already exists → {out.name}")
            return

        tag = d.strftime("%d%m%Y")
        url = BASE_URL.format(date=tag)

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)

            if r.status_code == 200 and len(r.content) > 50_000:
                out.write_bytes(r.content)
                print(f"  ✅ Downloaded & saved → {out.name}")
                return
            else:
                print("  ⚠️ Not available")

        except requests.RequestException as e:
            print(f"  ❌ Network error : {e}")

    raise RuntimeError("❌ No FO bhavcopy found in recent days")

# --------------------------------------------------
# CLI
# --------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download daily NSE FO options bhavcopy"
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
