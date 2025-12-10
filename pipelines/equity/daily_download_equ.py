#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY NIFTY50 EQUITY DOWNLOADER
RAW DATA ONLY — DO NOT CLEAN HERE
"""

from pathlib import Path
from datetime import datetime
import yfinance as yf
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

OUT_DIR = BASE / "data" / "raw" / "equity"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TICKER = "^NSEI"

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    print("🚀 NIFTY-LAB | DAILY EQUITY DOWNLOAD (NIFTY 50)")
    print("-" * 60)

    # ------------------------------
    # Ask date
    # ------------------------------
    date_str = input(
        "📅 Enter date (YYYY-MM-DD) [default = today]: "
    ).strip()

    if date_str == "":
        trade_date = datetime.today()
    else:
        try:
            trade_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD")
            return

    date_tag = trade_date.strftime("%Y-%m-%d")
    out_file = OUT_DIR / f"equity_{date_tag}.csv"

    if out_file.exists():
        print(f"⚠️ Already exists: {out_file.name}")
        return

    # ------------------------------
    # Download data
    # ------------------------------
    df = yf.download(
        TICKER,
        start=trade_date.strftime("%Y-%m-%d"),
        end=(trade_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        print("❌ No data returned (holiday or market closed)")
        return

    df = df.reset_index()

    # ✅ FIX: flatten MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    df.columns = (
        df.columns
        .str.upper()
        .str.replace(" ", "_")
    )

    df = df[["DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]]

    # ✅ Ensure exact trade date
    df = df[df["DATE"].dt.date == trade_date.date()]

    if df.empty:
        print("⚠️ Candle not available for this date")
        return

    df.to_csv(out_file, index=False)

    print("✅ Download successful")
    print(f"📅 Date  : {date_tag}")
    print(f"📦 Rows  : {len(df)}")
    print(f"💾 Saved : {out_file}")
    print("🎉 DONE ✅")


if __name__ == "__main__":
    main()
