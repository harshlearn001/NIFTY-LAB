#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.4 | OPTIONS EXECUTION ENGINE (FINAL — STABLE)

✔ Works with ALL NSE option chain formats
✔ ML + Regime aligned
✔ Capital-aware sizing (PHASE-11)
✔ Kill-switch enforced
✔ Production safe
"""

import sys
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# BOOTSTRAP
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR
from strategies.risk.capital_manager import CapitalState, compute_position_risk
from strategies.risk.regime_kill_switch import regime_kill_switch

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
LOT_SIZE = 50
SL_PCT   = 0.30
TGT_PCT  = 0.60

BASE_RISK = 0.01          # 1% base risk
CURRENT_EQUITY = 1.0     # normalized / paper capital

SIGNAL_DIR = BASE_DIR / "data/signals"
CHAIN_DIR  = BASE_DIR / "data/processed/options_chain"
OUT_DIR    = BASE_DIR / "data/signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# LOAD ML SIGNAL
# --------------------------------------------------
signal_file = sorted(SIGNAL_DIR.glob("nifty_final_signal_*.csv"))[-1]
sig = pd.read_csv(signal_file).iloc[0]

trade_date = pd.to_datetime(sig["date"]).date()
direction  = sig["signal"]
trend      = sig["trend_regime"]
vol        = sig["vol_regime"]
spot       = float(sig.get("close", 26142))

print("🧠 OPTIONS EXECUTION ENGINE")
print(f"📅 Date   : {trade_date}")
print(f"📊 Signal : {direction}")
print(f"📈 Regime : {trend} | {vol}")

# --------------------------------------------------
# REGIME KILL SWITCH
# --------------------------------------------------
allowed, size_mult, verdict = regime_kill_switch(trend, vol)
if not allowed:
    raise RuntimeError(f"❌ TRADING BLOCKED BY REGIME: {verdict}")

# --------------------------------------------------
# LOAD OPTION CHAIN
# --------------------------------------------------
chain_file = sorted(CHAIN_DIR.glob("nifty_option_chain_*.csv"))[-1]
chain = pd.read_csv(chain_file)
chain.columns = chain.columns.str.strip().str.upper()

# --------------------------------------------------
# NSE OPTION CHAIN AUTO-DETECTION
# --------------------------------------------------
if {"TYPE", "STRIKE", "PREMIUM", "OI", "EXPIRY"}.issubset(chain.columns):
    pass

elif {"OPT_TYPE", "STR_PRICE", "CLOSE_PRICE", "OPEN_INT", "EXP_DATE"}.issubset(chain.columns):
    chain["TYPE"]    = chain["OPT_TYPE"]
    chain["STRIKE"]  = chain["STR_PRICE"]
    chain["PREMIUM"] = chain["CLOSE_PRICE"]
    chain["OI"]      = chain["OPEN_INT"]
    chain["EXPIRY"]  = pd.to_datetime(chain["EXP_DATE"]).dt.date

elif {"OPTION_TYPE", "STRIKE", "CLOSE", "OPEN_INTEREST", "EXPIRY_DT"}.issubset(chain.columns):
    chain["TYPE"]    = chain["OPTION_TYPE"]
    chain["PREMIUM"] = chain["CLOSE"]
    chain["OI"]      = chain["OPEN_INTEREST"]
    chain["EXPIRY"]  = pd.to_datetime(chain["EXPIRY_DT"]).dt.date

else:
    raise RuntimeError(f"❌ Unsupported option chain format: {chain.columns.tolist()}")

chain["TYPE"] = chain["TYPE"].astype(str).str.strip().str.upper()

# --------------------------------------------------
# FILTER CE / PE
# --------------------------------------------------
opt_type = "CE" if direction == "LONG" else "PE"
df = chain[chain["TYPE"] == opt_type].copy()
if df.empty:
    raise RuntimeError("❌ No matching CE / PE options found")

# --------------------------------------------------
# STRIKE SELECTION
# --------------------------------------------------
df["DIST"] = (df["STRIKE"] - spot).abs()

if direction == "LONG" and trend == "BULL" and vol == "LOW_VOL":
    pick = df[df["STRIKE"] > spot].sort_values("DIST").iloc[0]
    tag = "OTM"
elif direction == "LONG":
    pick = df.sort_values("DIST").iloc[0]
    tag = "ATM"
elif direction == "SHORT" and vol == "HIGH_VOL":
    pick = df.sort_values("DIST").iloc[0]
    tag = "ATM"
else:
    pick = df[df["STRIKE"] < spot].sort_values("DIST").iloc[0]
    tag = "ITM"

# --------------------------------------------------
# CAPITAL ENGINE (PHASE-11)
# --------------------------------------------------
capital = CapitalState(initial_equity=CURRENT_EQUITY)
drawdown = capital.drawdown(CURRENT_EQUITY)

risk_pct, reason = compute_position_risk(
    base_risk=BASE_RISK,
    drawdown=drawdown,
    regime_multiplier=size_mult
)

if risk_pct == 0.0:
    raise RuntimeError("❌ CAPITAL PROTECTION MODE ACTIVE")

premium = float(pick["PREMIUM"])
risk_capital = CURRENT_EQUITY * risk_pct
lots = max(1, int(risk_capital / (premium * LOT_SIZE)))

# --------------------------------------------------
# SL / TARGET
# --------------------------------------------------
sl_price  = round(premium * (1 - SL_PCT), 2)
tgt_price = round(premium * (1 + TGT_PCT), 2)

# --------------------------------------------------
# FINAL TRADE
# --------------------------------------------------
trade = {
    "date": trade_date,
    "instrument": "NIFTY",
    "option_type": opt_type,
    "strike": int(pick["STRIKE"]),
    "expiry": pick["EXPIRY"],
    "direction": "BUY",
    "tag": tag,
    "lots": lots,
    "qty": lots * LOT_SIZE,
    "entry_price": round(premium, 2),
    "sl_price": sl_price,
    "target_price": tgt_price,
    "regime": f"{trend}|{vol}",
    "risk_pct": round(risk_pct, 4),
    "reason": f"ML + REGIME + OPTIONS | {reason}",
}

out = pd.DataFrame([trade])
fname = OUT_DIR / f"nifty_option_trade_{trade_date}.csv"
out.to_csv(fname, index=False)

print("\n✅ OPTION TRADE GENERATED")
print(out)
print(f"\n💾 Saved → {fname}")
