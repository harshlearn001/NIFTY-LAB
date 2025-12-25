#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-7.1 | TRADE ATTRIBUTION ANALYSIS (PRODUCTION SAFE)

✔ Explains WHY PnL happens
✔ No lookahead bias
✔ Plug-and-play with existing backtest output
✔ Zero dependency on live pipeline
"""

from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE_DIR = Path(r"H:\NIFTY-LAB-Trial")

BACKTEST_FILE = BASE_DIR / "data" / "backtest_nifty_results.csv"
ML_FILE       = BASE_DIR / "data" / "processed" / "ml" / "nifty_ml_prediction_historical.parquet"
OI_FILE       = BASE_DIR / "data" / "processed" / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE      = BASE_DIR / "data" / "processed" / "options_ml" / "nifty_pcr_daily.parquet"
REGIME_FILE   = BASE_DIR / "data" / "continuous" / "nifty_regime.parquet"

OUT_DIR = BASE_DIR / "data" / "analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "trade_attribution.csv"

print("🔍 PHASE-7.1 | BUILDING TRADE ATTRIBUTION")

# --------------------------------------------------
# LOAD DATA (SAFE)
# --------------------------------------------------
bt = pd.read_csv(BACKTEST_FILE)

ml = pd.read_parquet(ML_FILE)
oi = pd.read_parquet(OI_FILE)
pcr = pd.read_parquet(PCR_FILE)
reg = pd.read_parquet(REGIME_FILE)

# --------------------------------------------------
# NORMALIZE DATES
# --------------------------------------------------
def norm(df):
    for c in ["date", "DATE", "TRADE_DATE"]:
        if c in df.columns:
            df["date"] = pd.to_datetime(df[c]).dt.date
            return df
    raise RuntimeError("No date column found")

bt  = norm(bt)
ml  = norm(ml)
oi  = norm(oi)
pcr = norm(pcr)
reg = norm(reg)

# --------------------------------------------------
# OI REGIME COLUMN
# --------------------------------------------------
oi_reg_col = next(c for c in ["regime", "oi_regime", "oi_signal"] if c in oi.columns)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def ml_bucket(conf):
    if conf >= 0.80:
        return "VERY_HIGH"
    if conf >= 0.70:
        return "HIGH"
    if conf >= 0.60:
        return "MEDIUM"
    return "LOW"

def pcr_zone(val):
    if val > 1.1:
        return "OVERBOUGHT"
    if val < 0.9:
        return "OVERSOLD"
    return "NEUTRAL"

# --------------------------------------------------
# ATTRIBUTION LOOP
# --------------------------------------------------
rows = []

for _, trade in bt.iterrows():
    d = trade["date"]

    row_ml  = ml[ml["date"] <= d].sort_values("date").tail(1)
    row_oi  = oi[oi["date"] <= d].sort_values("date").tail(1)
    row_pcr = pcr[pcr["date"] <= d].sort_values("date").tail(1)
    row_reg = reg[reg["date"] <= d].sort_values("date").tail(1)

    if row_ml.empty or row_oi.empty or row_reg.empty:
        continue

    prob_up = float(row_ml["prob_up"].values[0])
    prob_dn = float(row_ml["prob_down"].values[0])
    conf    = max(prob_up, prob_dn)

    pcr_val = float(row_pcr["pcr"].values[0]) if not row_pcr.empty else 1.0

    rows.append({
        "date": d,
        "signal": trade["signal"],
        "ml_confidence": round(conf, 3),
        "ml_bucket": ml_bucket(conf),
        "oi_regime": row_oi.iloc[0][oi_reg_col],
        "pcr_zone": pcr_zone(pcr_val),
        "trend_regime": row_reg.iloc[0]["TREND_REGIME"],
        "vol_regime": row_reg.iloc[0]["VOL_REGIME"],
        "exit_reason": trade["exit_reason"],
        "pnl": trade["pnl"]
    })

# --------------------------------------------------
# SAVE
# --------------------------------------------------
attrib = pd.DataFrame(rows)
attrib.to_csv(OUT_FILE, index=False)

print("✅ TRADE ATTRIBUTION BUILT")
print(f"📁 Saved → {OUT_FILE}")
print("\n📊 SAMPLE")
print(attrib.head())
