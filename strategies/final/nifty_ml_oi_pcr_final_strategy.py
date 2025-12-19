#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL STRATEGY (PRODUCTION SAFE)
ML + FUTURES OI + OPTIONS PCR (EOD)

✔ As-of PCR alignment (CORRECT)
✔ Strict ML + OI date match
✔ Column-agnostic
✔ Never fails on date mismatch
"""

from pathlib import Path
import pandas as pd
import sys

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

ML_FILE  = BASE / "data" / "processed" / "ml" / "nifty_ml_prediction.parquet"
OI_FILE  = BASE / "data" / "processed" / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE = BASE / "data" / "processed" / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR = BASE / "data" / "signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print("🚀 NIFTY FINAL STRATEGY")

# --------------------------------------------------
# SAFE LOAD
# --------------------------------------------------
for f in [ML_FILE, OI_FILE, PCR_FILE]:
    if not f.exists():
        print(f"⚠️ Missing file : {f}")
        sys.exit(0)

ml  = pd.read_parquet(ML_FILE)
oi  = pd.read_parquet(OI_FILE)
pcr = pd.read_parquet(PCR_FILE)

# --------------------------------------------------
# DATE NORMALIZATION (PURE DATE)
# --------------------------------------------------
def normalize_date(df):
    for col in ["date", "TRADE_DATE", "DATE"]:
        if col in df.columns:
            df["date"] = pd.to_datetime(df[col]).dt.date
            return df
    raise KeyError(f"No date column in {df.columns.tolist()}")

ml  = normalize_date(ml)
oi  = normalize_date(oi)
pcr = normalize_date(pcr)

# --------------------------------------------------
# DETECT OI REGIME
# --------------------------------------------------
def detect_regime_column(df):
    for col in ["regime", "oi_regime", "oi_signal"]:
        if col in df.columns:
            return col
    raise KeyError(f"No regime column in OI file: {df.columns.tolist()}")

regime_col = detect_regime_column(oi)

# --------------------------------------------------
# ANCHOR DATE = ML DATE (KEY FIX)
# --------------------------------------------------
trade_date = ml["date"].max()

# Futures OI must match same date
# Futures OI = AS-OF (latest <= ML date)
oi_asof = oi[oi["date"] <= trade_date].sort_values("date").tail(1)

if oi_asof.empty:
    print(f"❌ No OI available on or before {trade_date}")
    sys.exit(0)


# PCR = AS-OF (latest <= trade_date)
pcr_asof = pcr[pcr["date"] <= trade_date].sort_values("date").tail(1)
if pcr_asof.empty:
    print(f"❌ No PCR available on or before {trade_date}")
    sys.exit(0)

# --------------------------------------------------
# EXTRACT VALUES
# --------------------------------------------------
row_ml  = ml[ml["date"] == trade_date].iloc[-1]
row_oi  = oi_asof.iloc[-1]

row_pcr = pcr_asof.iloc[-1]

prob_up   = float(row_ml["prob_up"])
prob_down = float(row_ml["prob_down"])
regime    = row_oi[regime_col]
pcr_val   = float(row_pcr["pcr"]) if pd.notna(row_pcr["pcr"]) else 1.0

# --------------------------------------------------
# DECISION ENGINE
# --------------------------------------------------
signal = "NO_TRADE"
reason = []

# LONG
if (
    prob_up >= 0.55 and
    regime in {"LONG_BUILDUP", "SHORT_COVERING"} and
    pcr_val <= 1.0
):
    signal = "LONG"
    reason = ["ML_UP", regime, f"PCR={pcr_val:.2f}"]

# SHORT
elif (
    prob_down >= 0.55 and
    regime in {"SHORT_BUILDUP", "LONG_UNWINDING"} and
    pcr_val >= 1.0
):
    signal = "SHORT"
    reason = ["ML_DOWN", regime, f"PCR={pcr_val:.2f}"]

# --------------------------------------------------
# POSITION SIZING
# --------------------------------------------------
position_size = 0.0

if signal == "LONG":
    position_size = 1.0 if prob_up >= 0.75 else 0.5 if prob_up >= 0.65 else 0.0
elif signal == "SHORT":
    position_size = 1.0 if prob_down >= 0.75 else 0.5 if prob_down >= 0.65 else 0.0

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
out = pd.DataFrame({
    "date": [trade_date],
    "signal": [signal],
    "position_size": [position_size],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
    "oi_regime": [regime],
    "pcr": [pcr_val],
    "reason": [" | ".join(reason)],
})

date_tag = pd.to_datetime(trade_date).strftime("%d-%m-%Y")
OUT_FILE = OUT_DIR / f"nifty_final_signal_{date_tag}.csv"

out.to_csv(OUT_FILE, index=False)

print("\n✅ FINAL SIGNAL GENERATED")
print(out)
print(f"\n💾 Saved : {OUT_FILE}")
