#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FIX MASTER EQUITY (HISTORICAL NORMALIZATION)
RUN ONCE ONLY
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")
MASTER_PQ = BASE / "data" / "continuous" / "master_equity.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_equity.csv"

def main():
    print("🛠️ FIXING MASTER EQUITY (HISTORICAL PATCH)")
    print("-" * 60)

    if not MASTER_PQ.exists():
        raise FileNotFoundError("❌ master_equity.parquet not found")

    df = pd.read_parquet(MASTER_PQ)
    print(f"✅ Loaded rows : {len(df):,}")

    # --------------------------------------------------
    # FIX SYMBOL
    # --------------------------------------------------
    if "SYMBOL" not in df.columns:
        df["SYMBOL"] = "NIFTY"
    else:
        df["SYMBOL"] = df["SYMBOL"].fillna("NIFTY")

    # --------------------------------------------------
    # FIX VOLUME
    # --------------------------------------------------
    if "VOLUME" not in df.columns:
        df["VOLUME"] = 0.0
    else:
        df["VOLUME"] = df["VOLUME"].fillna(0.0)

    # --------------------------------------------------
    # SORT & DEDUPE SAFETY
    # --------------------------------------------------
    df = (
        df
        .sort_values("DATE")
        .drop_duplicates(subset=["DATE", "SYMBOL"])
        .reset_index(drop=True)
    )

    df.to_parquet(MASTER_PQ, index=False)
    df.to_csv(MASTER_CSV, index=False)

    print("✅ MASTER EQUITY FIXED")
    print(f"📊 Rows   : {len(df):,}")
    print(f"📅 From   : {df['DATE'].min().date()}")
    print(f"📅 To     : {df['DATE'].max().date()}")
    print(f"💾 Saved  : {MASTER_PQ}")
    print("🎉 DONE ✅ (RUN ONCE ONLY)")

if __name__ == "__main__":
    main()
