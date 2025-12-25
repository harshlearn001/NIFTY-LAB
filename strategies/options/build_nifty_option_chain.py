#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.2 | NIFTY OPTION CHAIN BUILDER (INDEX OPTIONS)

✔ Uses OPTIDX
✔ NSE column safe
✔ Uses TRADE_DATE (FIXED)
"""

import sys
from pathlib import Path
import pandas as pd

# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR

print("🚀 BUILDING NIFTY OPTION CHAIN (INDEX OPTIONS)")

# --------------------------------------------------
# LOAD MASTER OPTIONS
# --------------------------------------------------
MASTER = BASE_DIR / "data/continuous/master_options.parquet"
df = pd.read_parquet(MASTER)

# --------------------------------------------------
# FILTER INDEX OPTIONS
# --------------------------------------------------
df = df[df["INSTRUMENT"] == "OPTIDX"].copy()

# --------------------------------------------------
# LATEST TRADE DATE (FIX)
# --------------------------------------------------
latest_date = df["TRADE_DATE"].max()
df = df[df["TRADE_DATE"] == latest_date]

# --------------------------------------------------
# NORMALIZE COLUMNS
# --------------------------------------------------
chain = pd.DataFrame({
    "TRADE_DATE": df["TRADE_DATE"],
    "EXPIRY_DT": df["EXP_DATE"],
    "STRIKE": df["STR_PRICE"],
    "OPTION_TYPE": df["OPT_TYPE"],
    "CLOSE": df["CLOSE_PRICE"],
    "OPEN_INTEREST": df["OPEN_INT"],
    "VOLUME": df["TRD_QTY"],
})

# --------------------------------------------------
# SAVE
# --------------------------------------------------
OUT_DIR = BASE_DIR / "data/processed/options_chain"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT = OUT_DIR / "nifty_option_chain_latest.csv"
chain.to_csv(OUT, index=False)

print("✅ OPTION CHAIN BUILT SUCCESSFULLY")
print(f"📅 Trade date : {latest_date}")
print(f"📦 Rows       : {len(chain)}")
print(f"💾 Saved → {OUT}")
