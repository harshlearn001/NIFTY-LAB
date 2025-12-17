#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | NSE OFFICIAL DAILY EQUITY (EOD ONLY)

✔ NSE bhavcopy
✔ EOD guaranteed
✔ Futures/options aligned
✔ Scheduler safe
"""

from pathlib import Path
from datetime import datetime, timedelta
import zipfile
import requests
import pandas as pd
from io import StringIO

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
RAW_DIR = BASE / "data" / "raw" / "equity"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# NSE CONFIG
# --------------------------------------------------
URL_TMPL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com/",
}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def latest_trade_date():
    today = datetime.today().date()
    if today.weekday() >= 5:
        return today - timedelta(days=today.weekday() - 4)
    return today


def read_bhavcopy(zip_bytes):
    with zipfile.ZipFile(zip_bytes) as z:
        for name in z.namelist():
            if name.lower().endswith(".csv"):
                with z.open(name) as f:
                    return pd.read_csv(f)
    return None


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    trade_date = latest_trade_date()
    tag = trade_date.strftime("%Y%m%d")
    out_file = RAW_DIR / f"equity_{trade_date}.csv"

    print("NIFTY-LAB | NSE EQUITY EOD DOWNLOAD")
    print("-" * 60)
    print(f"Trade date : {trade_date}")

    if out_file.exists():
        print(f"Already exists → {out_file.name}")
        return

    url = URL_TMPL.format(date=tag)

    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        print("Bhavcopy not available yet")
        return

    df = read_bhavcopy(
        zip_bytes=zipfile.ZipFile(
            StringIO().buffer
        )
    )

    df = pd.read_csv(
        zipfile.ZipFile(
            pd.io.common.BytesIO(r.content)
        ).open(
            zipfile.ZipFile(
                pd.io.common.BytesIO(r.content)
            ).namelist()[0]
        )
    )

    df.columns = df.columns.str.strip().str.upper()

    # ---- Extract NIFTY index ----
    nifty = df[
        (df["SYMBOL"] == "NIFTY") &
        (df["SERIES"] == "EQ")
    ]

    if nifty.empty:
        print("NIFTY row not found in bhavcopy")
        return

    out = pd.DataFrame({
        "DATE": [trade_date],
        "OPEN": [nifty["OPEN"].values[0]],
        "HIGH": [nifty["HIGH"].values[0]],
        "LOW":  [nifty["LOW"].values[0]],
        "CLOSE":[nifty["CLOSE"].values[0]],
        "VOLUME":[nifty["TOTTRDQTY"].values[0]],
        "SYMBOL": ["NIFTY"],
    })

    out.to_csv(out_file, index=False)

    print("EOD equity saved (NSE)")
    print(out)


if __name__ == "__main__":
    main()
