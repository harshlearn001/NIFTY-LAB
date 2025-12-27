#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Build NIFTY Daily Returns
------------------------------------
✔ Uses master_equity.csv
✔ Computes next-day returns
✔ Backtest-safe (no look-ahead)
"""

from pathlib import Path
import pandas as pd

# ==========================================================
# PATHS
# ==========================================================
BASE = Path(r"H:\NIFTY-LAB")
EQ_FILE = BASE / "data" / "continuous" / "master_equity.csv"
OUT_FILE = BASE / "data" / "backtest" / "nifty_daily_returns.csv"

OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

# ==========================================================
# LOAD
# ==========================================================
df = pd.read_csv(EQ_FILE, parse_dates=["DATE"])

# Only NIFTY
df = df[df["SYMBOL"] == "NIFTY"].copy()

df = df.sort_values("DATE").reset_index(drop=True)

# ==========================================================
# COMPUTE RETURNS
# ==========================================================
df["NEXT_CLOSE"] = df["CLOSE"].shift(-1)
df["RET"] = (df["NEXT_CLOSE"] - df["CLOSE"]) / df["CLOSE"]

df = df.dropna(subset=["RET"])

# ==========================================================
# SAVE
# ==========================================================
df_out = df[["DATE", "CLOSE", "NEXT_CLOSE", "RET"]]
df_out.to_csv(OUT_FILE, index=False)

print("✅ NIFTY daily returns built")
print(df_out.head())
print(f"\n📁 Saved to: {OUT_FILE}")
