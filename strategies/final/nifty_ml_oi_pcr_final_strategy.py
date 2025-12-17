#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL STRATEGY
ML + FUTURES OI + OPTIONS PCR (EOD)

✔ Probability-filtered
✔ Institution-aware
✔ No overtrading
✔ Production ready
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

ML_PRED_FILE = BASE / "data" / "processed" / "ml" / "nifty_ml_prediction.parquet"
OI_FILE      = BASE / "data" / "processed" / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE     = BASE / "data" / "processed" / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR = BASE / "data" / "signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_final_signal.csv"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
print("🚀 NIFTY FINAL STRATEGY")

ml  = pd.read_parquet(ML_PRED_FILE)
oi  = pd.read_parquet(OI_FILE)
pcr = pd.read_parquet(PCR_FILE)

# Standardise
oi  = oi.rename(columns={"TRADE_DATE": "date", "oi_signal": "regime"})
pcr = pcr[["date", "pcr"]]

ml["date"]  = pd.to_datetime(ml["date"])
oi["date"]  = pd.to_datetime(oi["date"])
pcr["date"] = pd.to_datetime(pcr["date"])

# --------------------------------------------------
# MERGE (STRICT DATE)
# --------------------------------------------------
df = (
    ml.merge(oi[["date", "regime"]], on="date", how="inner")
      .merge(pcr, on="date", how="inner")
)

if df.empty:
    raise RuntimeError("❌ No aligned ML/OI/PCR data")

row = df.iloc[0]

prob_up   = row["prob_up"]
prob_down = row["prob_down"]
regime    = row["regime"]
pcr_val   = row["pcr"]

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
# OUTPUT
# --------------------------------------------------
out = pd.DataFrame({
    "date": [row["date"]],
    "signal": [signal],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
    "oi_regime": [regime],
    "pcr": [pcr_val],
    "reason": [" | ".join(reason)]
})

out.to_csv(OUT_FILE, index=False)

print("\n✅ FINAL SIGNAL GENERATED")
print(out)
print(f"\n💾 Saved: {OUT_FILE}")
