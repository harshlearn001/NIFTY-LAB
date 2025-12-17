#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL STRATEGY (PRODUCTION SAFE)
ML + FUTURES OI + OPTIONS PCR (EOD)

✔ Column-agnostic
✔ Date-safe
✔ Skips gracefully
✔ Dated output
"""

from pathlib import Path
import pandas as pd

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
missing = False

if not ML_FILE.exists():
    print(f"⚠️ ML file missing : {ML_FILE}")
    missing = True

if not OI_FILE.exists():
    print(f"⚠️ OI file missing : {OI_FILE}")
    missing = True

if not PCR_FILE.exists():
    print(f"⚠️ PCR file missing : {PCR_FILE}")
    missing = True

if missing:
    print("⏭️ Skipping final strategy safely")
    exit(0)

ml  = pd.read_parquet(ML_FILE)
oi  = pd.read_parquet(OI_FILE)
pcr = pd.read_parquet(PCR_FILE)

# --------------------------------------------------
# DATE NORMALIZATION (CRITICAL FIX)
# --------------------------------------------------
def normalize_date(df):
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    elif "TRADE_DATE" in df.columns:
        df["date"] = pd.to_datetime(df["TRADE_DATE"])
    else:
        raise KeyError(f"No date column in {df.columns.tolist()}")
    return df

ml  = normalize_date(ml)
oi  = normalize_date(oi)
pcr = normalize_date(pcr)

# --------------------------------------------------
# STANDARDIZE OI + PCR
# --------------------------------------------------
oi  = oi.rename(columns={"oi_signal": "regime"})
pcr = pcr[["date", "pcr"]]

# --------------------------------------------------
# MERGE (STRICT DATE ALIGNMENT)
# --------------------------------------------------
df = (
    ml.merge(oi[["date", "regime"]], on="date", how="inner")
      .merge(pcr, on="date", how="inner")
)

if df.empty:
    print("❌ No aligned ML / OI / PCR rows")
    exit(0)

row = df.iloc[-1]

prob_up   = float(row["prob_up"])
prob_down = float(row["prob_down"])
regime    = row["regime"]
pcr_val   = float(row["pcr"]) if pd.notna(row["pcr"]) else 1.0

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
# OUTPUT (DATED)
# --------------------------------------------------
out = pd.DataFrame({
    "date": [row["date"]],
    "signal": [signal],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
    "oi_regime": [regime],
    "pcr": [pcr_val],
    "reason": [" | ".join(reason)],
})

date_tag = row["date"].strftime("%d-%m-%Y")
OUT_FILE = OUT_DIR / f"nifty_final_signal_{date_tag}.csv"

out.to_csv(OUT_FILE, index=False)

print("\n✅ FINAL SIGNAL GENERATED")
print(out)
print(f"\n💾 Saved : {OUT_FILE}")
