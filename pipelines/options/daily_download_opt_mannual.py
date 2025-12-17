#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY OPTIONS DOWNLOAD (FO ZIP)
RAW DATA ONLY — DO NOT CLEAN HERE
"""

from pathlib import Path
from datetime import datetime
import requests

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
def main():
    print("🚀 NIFTY-LAB | DAILY OPTIONS DOWNLOAD (FO ZIP)")
    print("-" * 60)

    inp = input("📅 Enter date (YYYY-MM-DD) [default=today]: ").strip()

    if not inp:
        trade_date = datetime.today()
    else:
        trade_date = datetime.strptime(inp, "%Y-%m-%d")

    tag = trade_date.strftime("%d%m%Y")
    out = OUT_DIR / f"fo_{trade_date.date()}.zip"
    url = BASE_URL.format(date=tag)

    print(f"🌐 URL  : {url}")

    if out.exists():
        print(f"⏩ Already downloaded: {out.name}")
        return

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)

        if r.status_code == 200 and len(r.content) > 50_000:
            out.write_bytes(r.content)
            print("✅ Download successful")
            print(f"📅 Date  : {trade_date.date()}")
            print(f"💾 Saved : {out}")
        else:
            print("❌ Bhavcopy not available (Holiday / Not released yet)")

    except Exception as e:
        print(f"⚠️ Download failed: {e}")

    print("🎉 DONE ✅")


if __name__ == "__main__":
    main()
