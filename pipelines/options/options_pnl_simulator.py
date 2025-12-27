#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-14 | OPTIONS PnL SIMULATOR (ULTRA SAFE)

✔ Schema tolerant
✔ NSE option chain auto-detect
✔ SL / Target / EOD exit
✔ Optional fields handled safely
✔ Backtest & live compatible
"""

import sys
from pathlib import Path
import pandas as pd

# ==================================================
# BOOTSTRAP
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR

# ==================================================
# PATHS
# ==================================================
TRADE_DIR = BASE_DIR / "data" / "signals"
CHAIN_DIR = BASE_DIR / "data" / "processed" / "options_chain"
OUT_DIR   = BASE_DIR / "data" / "backtest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# LOAD LATEST OPTION TRADE
# ==================================================
trade_file = sorted(TRADE_DIR.glob("nifty_option_trade_*.csv"))[-1]
trade_df = pd.read_csv(trade_file)
trade_df.columns = trade_df.columns.str.upper()
trade = trade_df.iloc[0]

# ==================================================
# LOAD OPTION CHAIN
# ==================================================
chain_file = sorted(CHAIN_DIR.glob("nifty_option_chain_*.csv"))[-1]
chain = pd.read_csv(chain_file)
chain.columns = chain.columns.str.strip().str.upper()

# ==================================================
# NSE OPTION CHAIN AUTO-DETECTION
# ==================================================
if {"TYPE", "STRIKE", "PREMIUM", "EXPIRY"}.issubset(chain.columns):
    pass

elif {"OPT_TYPE", "STR_PRICE", "CLOSE_PRICE", "OPEN_INT", "EXP_DATE"}.issubset(chain.columns):
    chain["TYPE"]    = chain["OPT_TYPE"]
    chain["STRIKE"]  = chain["STR_PRICE"]
    chain["PREMIUM"] = chain["CLOSE_PRICE"]
    chain["EXPIRY"]  = pd.to_datetime(chain["EXP_DATE"]).dt.date

elif {"OPTION_TYPE", "STRIKE", "CLOSE", "OPEN_INTEREST", "EXPIRY_DT"}.issubset(chain.columns):
    chain["TYPE"]    = chain["OPTION_TYPE"]
    chain["PREMIUM"] = chain["CLOSE"]
    chain["EXPIRY"]  = pd.to_datetime(chain["EXPIRY_DT"]).dt.date

else:
    raise RuntimeError(f"❌ Unsupported option chain schema: {chain.columns.tolist()}")

chain["TYPE"] = chain["TYPE"].astype(str).str.strip().str.upper()

# ==================================================
# LOCATE SAME OPTION
# ==================================================
mask = (
    (chain["TYPE"] == trade["OPTION_TYPE"]) &
    (chain["STRIKE"] == trade["STRIKE"]) &
    (pd.to_datetime(chain["EXPIRY"]).dt.date ==
     pd.to_datetime(trade["EXPIRY"]).date())
)

opt = chain.loc[mask]
if opt.empty:
    raise RuntimeError("❌ Option not found in option chain")

opt = opt.iloc[0]

# ==================================================
# PRICE DATA (SAFE)
# ==================================================
entry = float(trade["ENTRY_PRICE"])
high  = float(opt.get("HIGH", opt["PREMIUM"]))
low   = float(opt.get("LOW",  opt["PREMIUM"]))
close = float(opt["PREMIUM"])

sl  = float(trade["SL_PRICE"])
tgt = float(trade["TARGET_PRICE"])

# ==================================================
# EXIT LOGIC
# ==================================================
if low <= sl:
    exit_price = sl
    exit_reason = "STOP_LOSS"
elif high >= tgt:
    exit_price = tgt
    exit_reason = "TARGET"
else:
    exit_price = close
    exit_reason = "EOD_EXIT"

# ==================================================
# PnL
# ==================================================
qty = int(trade["QTY"])
pnl = round((exit_price - entry) * qty, 2)

# ==================================================
# OUTPUT (SAFE FIELDS)
# ==================================================
out = {
    "DATE": trade["DATE"],
    "OPTION_TYPE": trade["OPTION_TYPE"],
    "STRIKE": trade["STRIKE"],
    "EXPIRY": trade["EXPIRY"],
    "ENTRY": entry,
    "EXIT": exit_price,
    "QTY": qty,
    "PNL": pnl,
    "EXIT_REASON": exit_reason,
    "CONFIDENCE": trade.get("CONFIDENCE", None),
    "REGIME": trade.get("REGIME", None),
}

df = pd.DataFrame([out])
fname = OUT_DIR / "nifty_option_pnl.csv"
df.to_csv(fname, index=False)

print("\n✅ OPTION PnL SIMULATED")
print(df)
print(f"\n📁 Saved → {fname}")
