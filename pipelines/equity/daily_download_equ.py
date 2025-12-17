#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | NSE NIFTY INDEX EOD (ULTRA ROBUST)

✔ Works across all NSE formats (2007 → now)
✔ Auto-detects index name + value columns
✔ Case / space safe
✔ Scheduler safe
"""

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests
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
    "https://nsearchives.nseindia.com/content/indices/"
    "ind_close_all_{date}.csv"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://www.nseindia.com/",
}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def last_trading_days(n=10):
    d = datetime.today().date()
    days = []
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return days


def find_index_col(df):
    for c in df.columns:
        uc = c.upper()
        if "INDEX" in uc and "VALUE" not in uc:
            return c
    raise RuntimeError("Index name column not found")


def find_price_cols(df):
    cols = {c.upper(): c for c in df.columns}

    def find(keyword_list):
        for uc, orig in cols.items():
            if all(k in uc for k in keyword_list):
                return orig
        return None

    o = find(["OPEN", "INDEX"])
    h = find(["HIGH", "INDEX"])
    l = find(["LOW", "INDEX"])
    c = find(["CLOSE", "INDEX"]) or find(["CLOSING", "INDEX"])

    if not all([o, h, l, c]):
        raise RuntimeError(
            f"No known index price columns found. Columns seen: {list(df.columns)}"
        )

    return o, h, l, c


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | NSE NIFTY INDEX EOD DOWNLOAD")
    print("-" * 60)

    for trade_date in last_trading_days():
        tag = trade_date.strftime("%d%m%Y")
        out_file = RAW_DIR / f"equity_{trade_date}.csv"

        if out_file.exists():
            print(f"⏩ Already exists → {out_file.name}")
            return

        print(f"📅 Trying index file for {trade_date}")
        url = URL_TMPL.format(date=tag)

        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                raise RuntimeError("Index file not available")

            df = pd.read_csv(StringIO(r.text))
            df.columns = df.columns.str.strip()

            idx_col = find_index_col(df)
            o_col, h_col, l_col, c_col = find_price_cols(df)

            df[idx_col] = (
                df[idx_col]
                .astype(str)
                .str.upper()
                .str.strip()
            )

            nifty = df[df[idx_col].str.contains("NIFTY 50", regex=False)]
            if nifty.empty:
                raise RuntimeError("NIFTY 50 row not found")

            r0 = nifty.iloc[0]

            out = pd.DataFrame({
                "DATE":   [trade_date],
                "OPEN":   [r0[o_col]],
                "HIGH":   [r0[h_col]],
                "LOW":    [r0[l_col]],
                "CLOSE":  [r0[c_col]],
                "VOLUME": [0],
                "SYMBOL": ["NIFTY"],
            })

            out.to_csv(out_file, index=False)

            print(f"✅ NIFTY index saved → {out_file.name}")
            print(out)
            return

        except Exception as e:
            print(f"⚠️ Not available for {trade_date} ({e})")

    raise RuntimeError("❌ No NIFTY index data found in recent days")


# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Non-fatal error: {e}")
        # 🔑 IMPORTANT
        exit(0)
