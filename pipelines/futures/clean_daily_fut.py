#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY FUTURES (AUTO | NIFTY ONLY)

✔ Auto-finds latest FO ZIP
✔ FIXED: Reads NSE OPEN_INT* correctly
✔ Holiday / delay safe
✔ NEVER breaks scheduler
✔ Outputs Parquet + CSV
"""

from pathlib import Path
from datetime import datetime, timedelta
import zipfile
import pandas as pd
from io import StringIO

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")
RAW_DIR = BASE / "data" / "raw" / "futures"
OUT_DIR = BASE / "data" / "processed" / "daily" / "futures"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def find_latest_fo_zip(max_lookback=7):
    today = datetime.today().date()
    for i in range(max_lookback):
        d = today - timedelta(days=i)
        f = RAW_DIR / f"fo_{d:%Y-%m-%d}.zip"
        if f.exists():
            return d, f
    return None, None


def read_nse_csv(f):
    lines = f.read().decode("utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("INSTRUMENT"):
            return pd.read_csv(StringIO("\n".join(lines[i:])))
    return None


def normalize_columns(df):
    """
    NSE sometimes uses special chars like *, %, ₹ in column names.
    This function strips everything except A-Z, 0-9 and _.
    """
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace(r"[^A-Z0-9_]", "", regex=True)  # 🔥 KEY FIX
    )
    return df

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | CLEAN DAILY FUTURES (AUTO | NIFTY ONLY)")
    print("-" * 60)

    trade_date, zip_file = find_latest_fo_zip()

    if zip_file is None:
        print("No FO ZIP found in recent days — skipping futures clean")
        return

    out_pq = OUT_DIR / f"FUT_NIFTY_{trade_date}.parquet"
    out_csv = OUT_DIR / f"FUT_NIFTY_{trade_date}.csv"

    if out_pq.exists():
        print(f"Already cleaned → {out_pq.name}")
        return

    print(f"Using FO ZIP : {zip_file.name}")
    print(f"Trade Date  : {trade_date}")

    frames = []

    with zipfile.ZipFile(zip_file) as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue

            with z.open(name) as f:
                df = read_nse_csv(f)
                if df is None or df.empty:
                    continue

                df = normalize_columns(df)

                if not {"INSTRUMENT", "SYMBOL"}.issubset(df.columns):
                    continue

                df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip()
                df["SYMBOL"] = df["SYMBOL"].astype(str).str.strip()

                # Only NIFTY index futures
                df = df[
                    (df["INSTRUMENT"] == "FUTIDX") &
                    (df["SYMBOL"] == "NIFTY")
                ]

                if df.empty:
                    continue

                # Detect expiry column
                for c in ("EXP_DATE", "EXPIRY", "EXPIRY_DATE", "EXP_DT"):
                    if c in df.columns:
                        df.rename(columns={c: "EXP_DATE"}, inplace=True)
                        break
                else:
                    continue

                df["TRADE_DATE"] = trade_date
                frames.append(df)

    if not frames:
        print("No valid NIFTY futures found — skipping")
        return

    df = pd.concat(frames, ignore_index=True)

    # --------------------------------------------------
    # DATE NORMALIZATION
    # --------------------------------------------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"])
    df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True)

    # --------------------------------------------------
    # PRICE COLUMN NORMALIZATION
    # --------------------------------------------------
    df.rename(
        columns={
            "OPEN_PRICE": "OPEN",
            "HI_PRICE": "HIGH",
            "LO_PRICE": "LOW",
            "CLOSE_PRICE": "CLOSE",
            "TRD_QTY": "VOLUME",
        },
        inplace=True,
    )

    # --------------------------------------------------
    # ✅ CORRECT NSE OI HANDLING (NOW WORKS)
    # --------------------------------------------------
    OI_ALIASES = [
        "OPEN_INT",
        "OPENINTEREST",
        "OPEN_INTEREST",
        "OPEN_INT_QTY",
        "OI",
    ]

    oi_col = next((c for c in OI_ALIASES if c in df.columns), None)

    if oi_col:
        df["OI"] = pd.to_numeric(df[oi_col], errors="coerce")
    else:
        df["OI"] = pd.NA

    # --------------------------------------------------
    # SAFE COLUMN SELECTION
    # --------------------------------------------------
    required = [
        "SYMBOL",
        "TRADE_DATE",
        "EXP_DATE",
        "OPEN",
        "HIGH",
        "LOW",
        "CLOSE",
        "VOLUME",
        "OI",
    ]

    df = df[[c for c in required if c in df.columns]]

    # --------------------------------------------------
    # TYPE COERCION
    # --------------------------------------------------
    for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # --------------------------------------------------
    # CLEANING
    # --------------------------------------------------
    df = df.drop_duplicates()
    df = df.dropna(subset=["OPEN", "HIGH", "LOW", "CLOSE"])

    # --------------------------------------------------
    # SAVE
    # --------------------------------------------------
    df.to_parquet(out_pq, index=False)
    df.to_csv(out_csv, index=False)

    print(f"Saved : {out_pq.name}")
    print(f"Rows  : {len(df)}")
    print("✅ DAILY FUTURES CLEAN COMPLETE")

# --------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Non-fatal futures clean error: {e}")
        exit(0)
