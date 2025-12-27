#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.4 | OPTIONS EXECUTION ENGINE (FINAL — ML ALIGNED)

✔ Consumes NEW ML decision signal (ACTION / CONFIDENCE / CAPITAL)
✔ NSE option chain auto-detection
✔ Regime + confidence kill-switch
✔ Capital-aware risk sizing (₹ correct)
✔ SL / Target enforced
✔ Expiry safety enforced
✔ Production safe
"""

# ==========================================================
# BOOTSTRAP
# ==========================================================
import sys
from pathlib import Path
from datetime import date
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ==========================================================
# IMPORTS
# ==========================================================
from configs.paths import BASE_DIR
from strategies.risk.capital_manager import CapitalState, compute_position_risk
from strategies.risk.regime_kill_switch import regime_kill_switch

# ==========================================================
# CONFIG
# ==========================================================
LOT_SIZE = 50
SL_PCT   = 0.30
TGT_PCT  = 0.60

BASE_RISK = 0.01  # 1% base risk

SIGNAL_DIR = BASE_DIR / "data" / "signals"
CHAIN_DIR  = BASE_DIR / "data" / "processed" / "options_chain"
OUT_DIR    = BASE_DIR / "data" / "signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==========================================================
# LOAD LATEST ML SIGNAL
# ==========================================================
signal_file = sorted(SIGNAL_DIR.glob("nifty_final_signal_*.csv"))[-1]
sig_df = pd.read_csv(signal_file)
sig_df.columns = sig_df.columns.str.upper()
sig = sig_df.iloc[0]

# --- DATE (schema-safe)
if "DATE" in sig.index:
    trade_date = pd.to_datetime(sig["DATE"]).date()
elif "DATE_" in sig.index:
    trade_date = pd.to_datetime(sig["DATE_"]).date()
else:
    raise RuntimeError(f"❌ DATE column not found in signal: {sig.index.tolist()}")

# --- CORE FIELDS
ACTION     = str(sig.get("ACTION", "HOLD")).upper()
CONFIDENCE = float(sig.get("CONFIDENCE", 0.0))
CAPITAL    = float(sig.get("CAPITAL", 1_000_000))
REGIME     = str(sig.get("REGIME", "TREND")).upper()
spot       = float(sig.get("CLOSE", sig.get("SPOT", 26142)))

print("\n🧠 OPTIONS EXECUTION ENGINE")
print(f"📅 Date       : {trade_date}")
print(f"📊 Action     : {ACTION}")
print(f"🎯 Confidence : {CONFIDENCE:.3f}")
print(f"💰 Capital    : ₹{CAPITAL:,.0f}")
print(f"📈 Regime     : {REGIME}")

# ==========================================================
# CONFIDENCE KILL SWITCH
# ==========================================================
if ACTION == "HOLD" or CONFIDENCE < 0.60:
    raise RuntimeError("❌ NO OPTION TRADE: HOLD or low confidence signal")

# ==========================================================
# MAP REGIME → TREND / VOL
# ==========================================================
if REGIME == "TREND":
    trend, vol = "BULL", "LOW_VOL"
elif REGIME == "RANGE":
    trend, vol = "SIDEWAYS", "LOW_VOL"
else:
    trend, vol = "NEUTRAL", "HIGH_VOL"

# ==========================================================
# REGIME KILL SWITCH
# ==========================================================
allowed, size_mult, verdict = regime_kill_switch(trend, vol)
if not allowed:
    raise RuntimeError(f"❌ TRADING BLOCKED BY REGIME: {verdict}")

# ==========================================================
# LOAD OPTION CHAIN
# ==========================================================
chain_file = sorted(CHAIN_DIR.glob("nifty_option_chain_*.csv"))[-1]
chain = pd.read_csv(chain_file)
chain.columns = chain.columns.str.strip().str.upper()

# ==========================================================
# NSE OPTION CHAIN AUTO-DETECTION
# ==========================================================
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

# ==========================================================
# OPTION TYPE (SAFE)
# ==========================================================
if ACTION == "LONG":
    opt_type = "CE"
elif ACTION == "SHORT":
    opt_type = "PE"
else:
    raise RuntimeError("❌ Invalid ACTION for options execution")

df = chain[chain["TYPE"] == opt_type].copy()
if df.empty:
    raise RuntimeError("❌ No matching CE / PE options found")

# ==========================================================
# STRIKE SELECTION
# ==========================================================
df["DIST"] = (df["STRIKE"] - spot).abs()

if ACTION == "LONG" and trend == "BULL" and vol == "LOW_VOL" and CONFIDENCE > 0.75:
    pick = df[df["STRIKE"] > spot].sort_values("DIST").iloc[0]
    tag = "ITM"
else:
    pick = df.sort_values("DIST").iloc[0]
    tag = "ATM"

# ==========================================================
# EXPIRY SAFETY (MANDATORY)
# ==========================================================
if pick["EXPIRY"] <= date.today():
    raise RuntimeError("❌ Option expiry is today or expired — trade blocked")

# ==========================================================
# CAPITAL ENGINE (₹ REAL)
# ==========================================================
capital_state = CapitalState(initial_equity=CAPITAL)
drawdown = capital_state.drawdown(CAPITAL)

risk_pct, reason = compute_position_risk(
    base_risk=BASE_RISK,
    drawdown=drawdown,
    regime_multiplier=size_mult
)

if risk_pct <= 0:
    raise RuntimeError("❌ CAPITAL PROTECTION MODE ACTIVE")

premium = float(pick["PREMIUM"])
risk_capital = CAPITAL * risk_pct

lots = int(risk_capital / (premium * LOT_SIZE))
if lots <= 0:
    raise RuntimeError("❌ Risk too small → zero lots")

# ==========================================================
# SL / TARGET
# ==========================================================
sl_price  = round(premium * (1 - SL_PCT), 2)
tgt_price = round(premium * (1 + TGT_PCT), 2)

# ==========================================================
# FINAL TRADE OBJECT
# ==========================================================
trade = {
    "DATE": trade_date,
    "SYMBOL": "NIFTY",
    "ACTION": "BUY",
    "OPTION_TYPE": opt_type,
    "STRIKE": int(pick["STRIKE"]),
    "EXPIRY": pick["EXPIRY"],
    "TAG": tag,
    "LOTS": lots,
    "QTY": lots * LOT_SIZE,
    "ENTRY_PRICE": round(premium, 2),
    "SL_PRICE": sl_price,
    "TARGET_PRICE": tgt_price,
    "CAPITAL": round(CAPITAL, 2),
    "RISK_PCT": round(risk_pct, 4),
    "CONFIDENCE": round(CONFIDENCE, 4),
    "REGIME": f"{trend}|{vol}",
    "REASON": f"ML + REGIME + OPTIONS | {reason}",
}

# ==========================================================
# SAVE
# ==========================================================
out = pd.DataFrame([trade])
fname = OUT_DIR / f"nifty_option_trade_{trade_date}.csv"
out.to_csv(fname, index=False)

print("\n✅ OPTION TRADE GENERATED")
print(out)
print(f"\n💾 Saved → {fname}")
