#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CLEAN DAILY OPTIONS (NIFTY ONLY - FINAL FIX)

✅ NSE-safe spacing fix
✅ Handles junk headers
✅ ZIP filename → TRADE_DATE
✅ OPTIDX + NIFTY only
✅ Keeps dates as datetime64[ns] (NO int64 nanoseconds)
"""

from pathlib import Path
import zipfile
import pandas as pd
import re
from io import StringIO

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

RAW_DIR = BASE / "data" / "raw" / "options"
OUT_DIR = BASE / "data" / "processed" / "daily" / "options"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def extract_trade_date(fname: str):
    m = re.search(r"(\d{4}-\d{2}-\d{2})", fname)
    return pd.to_datetime(m.group(1)) if m else None


def read_nse_csv(f):
    """Skip junk header lines until the INSTRUMENT row."""
    lines = f.read().decode("utf-8", errors="ignore").splitlines()
    for i, line in enumerate(lines):
        if line.strip().startswith("INSTRUMENT"):
            return pd.read_csv(StringIO("\n".join(lines[i:])))
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
        .str.replace("*", "", regex=False)
    )
    return df


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("🧹 NIFTY-LAB | CLEAN DAILY OPTIONS (NIFTY ONLY)")
    print("-" * 60)

    zip_files = sorted(RAW_DIR.glob("fo_*.zip"))
    if not zip_files:
        print("⚠️ No FO ZIP files found")
        return

    for zpath in zip_files:
        trade_date = extract_trade_date(zpath.name)
        if trade_date is None:
            continue

        out = OUT_DIR / f"OPTIONS_NIFTY_{trade_date.date()}.parquet"
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

                    # --- strong filters ---
                    df["INSTRUMENT"] = df["INSTRUMENT"].astype(str).str.strip().str.upper()
                    df["SYMBOL"]     = df["SYMBOL"].astype(str).str.strip().str.upper()

                    df = df[
                        (df["INSTRUMENT"] == "OPTIDX") &
                        (df["SYMBOL"] == "NIFTY")
                    ]
                    if df.empty:
                        continue

                    # --- expiry column normalisation ---
                    for c in ("EXP_DATE", "EXPIRY", "EXPIRY_DATE", "EXP_DT"):
                        if c in df.columns:
                            df.rename(columns={c: "EXP_DATE"}, inplace=True)
                            break
                    else:
                        # no expiry column
                        continue

                    # add trade date
                    df["TRADE_DATE"] = trade_date

                    frames.append(df)

        if not frames:
            print("⚠️ No valid NIFTY options found")
            continue

        df = pd.concat(frames, ignore_index=True)

        # ---- dates as datetime ----
       # ✅ FINAL DATE NORMALIZATION (CRITICAL)
        df["EXP_DATE"] = pd.to_datetime(df["EXP_DATE"], errors="coerce")

        df["TRADE_DATE"] = pd.to_datetime(
            df["TRADE_DATE"], errors="coerce"
)


        # ---- numeric columns ONLY (do NOT touch dates) ----
        numeric_cols = [
            "STR_PRICE",
            "OPEN_PRICE",
            "HI_PRICE",
            "LO_PRICE",
            "CLOSE_PRICE",
            "OPEN_INT",
            "TRD_QTY",
            "NO_OF_CONT",
            "NO_OF_TRADE",
            "NOTION_VAL",
            "PR_VAL",
        ]
        for c in numeric_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # ---- final clean ----
        df = (
            df.dropna(subset=["TRADE_DATE", "EXP_DATE", "STR_PRICE"])
              .sort_values(["TRADE_DATE", "EXP_DATE", "STR_PRICE", "OPT_TYPE"])
              .drop_duplicates()
        )

        df.to_parquet(out, index=False)
        df.to_csv(out.with_suffix(".csv"), index=False)

        print(f"✅ Saved {out.name} | rows: {len(df):,}")

    print("\n🎉 DAILY OPTIONS CLEAN COMPLETE ✅")


if __name__ == "__main__":
    main()
