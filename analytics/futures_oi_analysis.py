#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FUTURES OI ANALYTICS (NIFTY)

AUTO • HOLIDAY SAFE • SCHEDULER SAFE

✔ Uses ALL available FUT_NIFTY files
✔ Builds multi-day OI analytics
✔ ML + Strategy ready
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

FUT_DAILY_DIR = BASE / "data" / "processed" / "daily" / "futures"
OUT_DIR = BASE / "data" / "processed" / "futures_ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_fut_oi_daily.parquet"
OUT_CSV = OUT_DIR / "nifty_fut_oi_daily.csv"

# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | FUTURES OI ANALYTICS")
    print("-" * 60)

    files = sorted(FUT_DAILY_DIR.glob("FUT_NIFTY_*.parquet"))

    if not files:
        print("No futures data available. Skipping.")
        return

    frames = []

    for f in files:
        df = pd.read_parquet(f)
        if df.empty:
            continue
        frames.append(df)

    if not frames:
        print("No valid futures data found after loading.")
        return

    # --------------------------------------------------
    # Combine all days
    # --------------------------------------------------
    df = pd.concat(frames, ignore_index=True)

    # --------------------------------------------------
    # Ensure correct dtypes
    # --------------------------------------------------
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce")
    df["EXP_DATE"]   = pd.to_datetime(df["EXP_DATE"], errors="coerce")
    df["OI"]         = pd.to_numeric(df["OI"], errors="coerce")
    df["CLOSE"]      = pd.to_numeric(df["CLOSE"], errors="coerce")

    df = df.dropna(subset=["TRADE_DATE", "EXP_DATE", "OI", "CLOSE"])

    if df.empty:
        print("No valid futures rows after cleaning.")
        return

    # --------------------------------------------------
    # Front-month selection (per day)
    # --------------------------------------------------
    front = (
        df.sort_values("EXP_DATE")
          .groupby("TRADE_DATE", as_index=False)
          .first()
          .sort_values("TRADE_DATE")
    )

    # --------------------------------------------------
    # OI FEATURES
    # --------------------------------------------------
    front["oi"] = front["OI"]
    front["oi_change"] = front["oi"].diff()
    front["oi_pct"] = front["oi"].pct_change()

    front["price"] = front["CLOSE"]
    front["price_change"] = front["price"].diff()
    front["price_pct"] = front["price"].pct_change()

    # --------------------------------------------------
    # OI REGIME CLASSIFICATION
    # --------------------------------------------------
    def classify(row):
        if row["price_change"] > 0 and row["oi_change"] > 0:
            return "LONG_BUILDUP"
        if row["price_change"] < 0 and row["oi_change"] > 0:
            return "SHORT_BUILDUP"
        if row["price_change"] > 0 and row["oi_change"] < 0:
            return "SHORT_COVERING"
        if row["price_change"] < 0 and row["oi_change"] < 0:
            return "LONG_UNWINDING"
        return "NEUTRAL"

    front["regime"] = front.apply(classify, axis=1)

    # --------------------------------------------------
    # Final output
    # --------------------------------------------------
    out = front[[
        "TRADE_DATE",
        "price",
        "price_pct",
        "oi_pct",
        "regime",
    ]].dropna()

    if out.empty:
        print("No analytics rows produced.")
        return

    # --------------------------------------------------
    # Save
    # --------------------------------------------------
    out.to_parquet(OUT_PQ, index=False)
    out.to_csv(OUT_CSV, index=False)

    print("Futures OI analytics built successfully")
    print(f"Rows : {len(out)}")
    print(f"From : {out['TRADE_DATE'].min().date()}")
    print(f"To   : {out['TRADE_DATE'].max().date()}")
    print(f"Saved: {OUT_PQ}")

# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":
    main()
