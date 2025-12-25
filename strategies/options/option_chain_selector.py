#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.3 | OPTION CHAIN SELECTOR (FINAL FIXED)

✔ Uses correct column names
✔ CE / PE logic
✔ OTM / ATM / ITM selection
✔ Regime aware
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# CONFIG (INPUTS)
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB-Trial")
CHAIN_DIR = BASE / "data/processed/options_chain"

spot_price = 26142
signal = "LONG"          # LONG / SHORT
expiry_type = "WEEKLY"   # (reserved for next phase)
trend = "BULL"
vol = "LOW_VOL"

# --------------------------------------------------
# LOAD LATEST OPTION CHAIN
# --------------------------------------------------
files = sorted(CHAIN_DIR.glob("nifty_option_chain_*.csv"))
if not files:
    raise FileNotFoundError(f"❌ No option chain found in {CHAIN_DIR}")

chain = pd.read_csv(files[-1])
print("OPTION_TYPE values:", chain["OPTION_TYPE"].unique()[:10])

# --------------------------------------------------
# FILTER CE / PE  (FIXED COLUMN NAME)
# --------------------------------------------------
opt_type = "CE" if signal == "LONG" else "PE"
chain["OPTION_TYPE"] = (
    chain["OPTION_TYPE"]
    .astype(str)
    .str.strip()
    .str.upper()
)

df = chain[chain["OPTION_TYPE"] == opt_type].copy()


if df.empty:
    raise RuntimeError("❌ No matching CE / PE options found")

# --------------------------------------------------
# STRIKE DISTANCE
# --------------------------------------------------
df["DIST"] = abs(df["STRIKE"] - spot_price)

# --------------------------------------------------
# STRIKE SELECTION LOGIC
# --------------------------------------------------
if signal == "LONG" and trend == "BULL" and vol == "LOW_VOL":
    pick = df[df["STRIKE"] > spot_price].sort_values("DIST").iloc[0]
    tag = "OTM"

elif signal == "LONG":
    pick = df.sort_values("DIST").iloc[0]
    tag = "ATM"

elif signal == "SHORT" and vol == "HIGH_VOL":
    pick = df.sort_values("DIST").iloc[0]
    tag = "ATM"

else:
    pick = df[df["STRIKE"] < spot_price].sort_values("DIST").iloc[0]
    tag = "ITM"

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
print("\n✅ OPTION SELECTED")
print("----------------------------------")
print(f"TYPE      : {opt_type}")
print(f"STRIKE    : {int(pick['STRIKE'])}")
print(f"TAG       : {tag}")
print(f"PREMIUM   : {round(pick['CLOSE'], 2)}")
print(f"OI        : {int(pick['OPEN_INTEREST'])}")
