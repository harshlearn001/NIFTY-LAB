#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY FUTURES DOWNLOAD (SINGLE DATE)

Downloads:
  foDDMMYYYY.zip from NSE

Saves:
  data/raw/futures/fo_YYYY-MM-DD.zip

RAW ONLY — DO NOT CLEAN HERE
"""

import requests
from datetime import datetime
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
def main():
    print("🚀 NIFTY-LAB | DAILY FUTURES DOWNLOAD (FO ZIP)")
    print("-" * 60)

    date_str = input(
        "📅 Enter date (YYYY-MM-DD) [default=today]: "
    ).strip()

    if date_str:
        trade_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        trade_date = datetime.today().date()

    if trade_date.weekday() >= 5:
        print("⚠️ Weekend selected — no FO data on Saturday/Sunday")
        return

    d_nse = trade_date.strftime("%d%m%Y")
    url = BASE_URL.format(date=d_nse)

    out_file = OUT_DIR / f"fo_{trade_date:%Y-%m-%d}.zip"

    if out_file.exists():
        print(f"⏩ Already downloaded: {out_file.name}")
        return

    print(f"🌐 URL  : {url}")

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 200 and len(r.content) > 50_000:
            with open(out_file, "wb") as f:
                f.write(r.content)

            print("✅ Download successful")
            print(f"📅 Date  : {trade_date}")
            print(f"💾 Saved : {out_file}")
            print("🎉 DONE ✅")
        else:
            print("❌ Bhavcopy not available (holiday / not released yet)")

    except Exception as e:
        print(f"⚠️ Error: {e}")


if __name__ == "__main__":
    main()
