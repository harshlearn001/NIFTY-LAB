#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY FUTURES (NIFTY ONLY - FINAL FIX)

✅ NSE-safe spacing fix
✅ Handles nanosecond dates
✅ Extracts ONLY NIFTY index futures
✅ Produces ML + master-ready output
"""

from pathlib import Path
import zipfile
import pandas as pd
import re
from io import StringIO

BASE = Path(r"H:\NIFTY-LAB")

RAW_DIR = BASE / "data" / "raw" / "futures"
OUT_DIR = BASE / "data" / "processed" / "daily" / "futures"
OUT_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def extract_trade_date(fname):
    m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
    return pd.to_datetime(m.group(1)) if m else None


def normalize_columns(df):
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace("*", "", regex=False)
    )
    return df


def read_nse_csv(f):
    lines = f.read().decode("utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("INSTRUMENT"):
            return pd.read_csv(StringIO("\n".join(lines[i:])))
    return None


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("🧹 NIFTY-LAB | CLEAN DAILY FUTURES (NIFTY ONLY)")
    print("-" * 60)

    zip_files = sorted(RAW_DIR.glob("fo_*.zip"))
    if not zip_files:
        print("⚠️ No FO ZIP files found")
        return

    for zpath in zip_files:
        trade_date = extract_trade_date(zpath.name)
        if trade_date is None:
            continue

        out = OUT_DIR / f"FUT_NIFTY_{trade_date.date()}.parquet"
        if out.exists():
            print(f"⏩ Already cleaned: {out.name}")
            continue

        print(f"\n📦 Processing: {zpath.name}")
        frames = []

        with zipfile.ZipFile(zpath) as z:
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

                    # ✅ CRITICAL FIX (THIS WAS MISSING)
                    df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip().str.upper()
                    df["SYMBOL"]     = df["SYMBOL"].astype(str).str.strip().str.upper()

                    # ✅ TRUE NIFTY FILTER
                    df = df[
                        (df["INSTRUMENT"] == "FUTIDX") &
                        (df["SYMBOL"] == "NIFTY")
                    ]

                    if df.empty:
                        continue

                    # expiry column detection
                    for c in ("EXP_DATE", "EXPIRY", "EXPIRY_DATE", "EXP_DT"):
                        if c in df.columns:
                            df.rename(columns={c: "EXP_DATE"}, inplace=True)
                            break
                    else:
                        continue

                    df["TRADE_DATE"] = trade_date
                    frames.append(df)

        if not frames:
            print("⚠️ No valid NIFTY futures found")
            continue

        df = pd.concat(frames, ignore_index=True)

        # ---- normalize to final schema ----
        df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
        df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], dayfirst=True, errors="coerce")

        df.rename(
            columns={
                "OPEN_PRICE": "OPEN",
                "HI_PRICE": "HIGH",
                "LO_PRICE": "LOW",
                "CLOSE_PRICE": "CLOSE",
                "TRD_QTY": "VOLUME",
                "OPEN_INT": "OI",
            },
            inplace=True,
        )

        df = df[
            ["SYMBOL", "TRADE_DATE", "EXP_DATE",
             "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "OI"]
        ]

        for c in ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "OI"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = (
            df.dropna()
            .sort_values(["TRADE_DATE", "EXP_DATE"])
            .drop_duplicates()
        )

        df.to_parquet(out, index=False)
        df.to_csv(out.with_suffix(".csv"), index=False)

        print(f"✅ Saved {out.name} | rows: {len(df)}")

    print("\n🎉 DAILY FUTURES CLEAN COMPLETE ✅")


if __name__ == "__main__":
    main()
