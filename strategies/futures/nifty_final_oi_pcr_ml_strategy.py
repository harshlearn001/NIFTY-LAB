#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FINAL NIFTY SYSTEM (PRODUCTION READY)
====================================
- Direction  : OI + PCR strategy
- Filter     : ML probability
- Risk       : ATR-based SL & Target (recomputed here)

Output:
- BUY / SELL / NO_TRADE
- Entry, Stop Loss, Target
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
import numpy as np
from configs.paths import PROC_DIR

# =================================================
# PATHS
# =================================================
OI_PCR_FILE = PROC_DIR / "futures_ml" / "nifty_oi_pcr_trade_signals.parquet"
ML_FILE     = PROC_DIR / "ml" / "nifty_ml_predictions.parquet"

OUT_FILE = PROC_DIR / "futures_ml" / "nifty_final_trade_signals.parquet"
OUT_CSV  = OUT_FILE.with_suffix(".csv")

# =================================================
# PARAMETERS (LOCKED)
# =================================================
BUY_PROB  = 0.65
SELL_PROB = 0.35
ATR_PERIOD = 14
ATR_SL  = 1.0
ATR_TGT = 2.0

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading OI+PCR signals and ML predictions...")

oi = pd.read_parquet(OI_PCR_FILE)
ml = pd.read_parquet(ML_FILE)

oi["date"] = pd.to_datetime(oi["date"])
ml["date"] = pd.to_datetime(ml["date"])

# =================================================
# MERGE ML PROBABILITY
# =================================================
df = oi.merge(
    ml[["date", "ml_prob"]],
    on="date",
    how="left"
)

# =================================================
# RECOMPUTE ATR (SAFE & CORRECT)
# =================================================
df = df.sort_values("date").reset_index(drop=True)

df["prev_close"] = df["close"].shift(1)
df["tr"] = (df["close"] - df["prev_close"]).abs()
df["atr"] = df["tr"].rolling(ATR_PERIOD).mean()
df["atr_pct"] = df["atr"] / df["close"]

# =================================================
# FINAL SIGNAL LOGIC
# =================================================
df["final_signal"] = "NO_TRADE"

# BUY
df.loc[
    (df["signal"] == "BUY") &
    (df["ml_prob"] >= BUY_PROB),
    "final_signal"
] = "BUY"

# SELL
df.loc[
    (df["signal"] == "SELL") &
    (df["ml_prob"] <= SELL_PROB),
    "final_signal"
] = "SELL"

# =================================================
# STOP LOSS & TARGET
# =================================================
df["stop_loss"] = np.nan
df["target"]   = np.nan

# BUY
df.loc[df["final_signal"] == "BUY", "stop_loss"] = (
    df["close"] - ATR_SL * df["atr"]
)
df.loc[df["final_signal"] == "BUY", "target"] = (
    df["close"] + ATR_TGT * df["atr"]
)

# SELL
df.loc[df["final_signal"] == "SELL", "stop_loss"] = (
    df["close"] + ATR_SL * df["atr"]
)
df.loc[df["final_signal"] == "SELL", "target"] = (
    df["close"] - ATR_TGT * df["atr"]
)

# =================================================
# FINAL OUTPUT
# =================================================
out_cols = [
    "date",
    "final_signal",
    "close",
    "stop_loss",
    "target",
    "ml_prob",
    "confidence",
    "regime",
]

df_out = df[out_cols].dropna(subset=["final_signal"]).copy()

# =================================================
# SAVE
# =================================================
df_out.to_parquet(OUT_FILE, index=False)
df_out.to_csv(OUT_CSV, index=False)

# =================================================
# SUMMARY
# =================================================
print("\n✅ FINAL OI + PCR + ML STRATEGY BUILT")
print(f"📦 Parquet: {OUT_FILE}")
print(f"📄 CSV    : {OUT_CSV}")
print("\n📊 Signal distribution:")
print(df_out["final_signal"].value_counts())
print("\n🎯 Sample:")
print(df_out.tail(5))
