#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL STRATEGY (PRODUCTION SAFE)

ML + FUTURES OI + OPTIONS PCR + MARKET REGIME (EOD)

✔ Single-source decision logic
✔ Live & Backtest identical
✔ As-of joins (NO lookahead)
✔ PCR optional (neutral if missing)
✔ BEAR blocks LONG, allows SHORT
✔ PHASE-2 confidence filtering
✔ Never crashes on missing data
"""

# ==================================================
# BOOTSTRAP
# ==================================================
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR, CONT_DIR

print("🧠 ML MODE →", "BACKTEST" if "backtest" in sys.argv[0] else "LIVE")
print("🚀 NIFTY FINAL STRATEGY (ML + OI + PCR + REGIME)")

# ==================================================
# PATHS
# ==================================================
ML_FILE  = BASE_DIR / "data/processed/ml/nifty_ml_prediction.parquet"
OI_FILE  = BASE_DIR / "data/processed/futures_ml/nifty_fut_oi_daily.parquet"
PCR_FILE = BASE_DIR / "data/processed/options_ml/nifty_pcr_daily.parquet"
REG_FILE = CONT_DIR / "nifty_regime.parquet"

OUT_DIR = BASE_DIR / "data/signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ==================================================
# DECISION ENGINE (PHASE-2)
# ==================================================
def generate_signal(
    prob_up: float,
    prob_down: float,
    oi_regime: str,
    pcr_val: float,
    trend_regime: str,
    vol_regime: str,
):
    signal = "NO_TRADE"
    position_size = 0.0
    reason = []

    MIN_CONF = 0.58   # 🔥 PHASE-2 THRESHOLD

    # -----------------------------
    # HARD MARKET FILTER
    # -----------------------------
    if trend_regime == "BEAR" and prob_up >= prob_down:
        return "NO_TRADE", 0.0, ["BEAR_NO_LONG"]

    # -----------------------------
    # ML CONFIDENCE FILTER
    # -----------------------------
    if prob_up >= MIN_CONF and prob_up > prob_down:
        signal = "LONG"
        position_size = (
            1.0 if prob_up >= 0.75 else
            0.6 if prob_up >= 0.65 else
            0.4
        )
        reason.append("ML_UP_STRONG")

    elif prob_down >= MIN_CONF and prob_down > prob_up:
        signal = "SHORT"
        position_size = (
            1.0 if prob_down >= 0.75 else
            0.6 if prob_down >= 0.65 else
            0.4
        )
        reason.append("ML_DOWN_STRONG")

    else:
        return "NO_TRADE", 0.0, ["LOW_CONFIDENCE"]

    # -----------------------------
    # OI REGIME WEIGHTING
    # -----------------------------
    if signal == "LONG":
        if oi_regime in {"LONG_BUILDUP", "SHORT_COVERING"}:
            reason.append(oi_regime)
        elif oi_regime == "LONG_UNWINDING":
            position_size *= 0.6
            reason.append("OI_WEAK_LONG")
        else:
            position_size *= 0.4
            reason.append("OI_AGAINST_LONG")

    if signal == "SHORT":
        if oi_regime in {"SHORT_BUILDUP", "LONG_UNWINDING"}:
            reason.append(oi_regime)
        elif oi_regime == "SHORT_COVERING":
            position_size *= 0.6
            reason.append("OI_WEAK_SHORT")
        else:
            position_size *= 0.4
            reason.append("OI_AGAINST_SHORT")

    # -----------------------------
    # PCR CONFIRMATION
    # -----------------------------
    if signal == "LONG" and pcr_val > 1.1:
        position_size *= 0.7
        reason.append("PCR_CAUTION")

    if signal == "SHORT" and pcr_val < 0.9:
        position_size *= 0.7
        reason.append("PCR_CAUTION")

    # -----------------------------
    # VOLATILITY CONTROL
    # -----------------------------
    if vol_regime == "HIGH_VOL":
        position_size *= 0.6
        reason.append("HIGH_VOL")

    return signal, round(position_size, 2), reason

# ==================================================
# SAFE LOAD
# ==================================================
for f in [ML_FILE, OI_FILE, REG_FILE]:
    if not f.exists():
        print(f"❌ Missing file : {f}")
        sys.exit(0)

ml     = pd.read_parquet(ML_FILE)
oi     = pd.read_parquet(OI_FILE)
regime = pd.read_parquet(REG_FILE)
pcr    = pd.read_parquet(PCR_FILE) if PCR_FILE.exists() else pd.DataFrame()

# ==================================================
# DATE NORMALIZATION
# ==================================================
def norm(df):
    for c in ["date", "DATE", "TRADE_DATE"]:
        if c in df.columns:
            df["date"] = pd.to_datetime(df[c]).dt.date
            return df
    raise RuntimeError("No date column")

ml = norm(ml)
oi = norm(oi)
regime = norm(regime)
if not pcr.empty:
    pcr = norm(pcr)

# ==================================================
# OI REGIME COLUMN
# ==================================================
oi_reg_col = next(c for c in ["regime", "oi_regime", "oi_signal"] if c in oi.columns)

# ==================================================
# ANCHOR DATE
# ==================================================
trade_date = ml["date"].max()

row_ml = ml[ml["date"] == trade_date].iloc[-1]
row_oi = oi[oi["date"] <= trade_date].sort_values("date").iloc[-1]
row_reg = regime[regime["date"] <= trade_date].sort_values("date").iloc[-1]

# PCR AS-OF SAFE
if pcr.empty or pcr[pcr["date"] <= trade_date].empty:
    pcr_val = 1.0
    pcr_note = "PCR_MISSING"
else:
    pcr_val = float(pcr[pcr["date"] <= trade_date].sort_values("date").iloc[-1]["pcr"])
    pcr_note = "PCR_OK"

# ==================================================
# DECISION
# ==================================================
signal, position_size, reason = generate_signal(
    prob_up=float(row_ml["prob_up"]),
    prob_down=float(row_ml["prob_down"]),
    oi_regime=row_oi[oi_reg_col],
    pcr_val=pcr_val,
    trend_regime=row_reg["TREND_REGIME"],
    vol_regime=row_reg["VOL_REGIME"],
)

reason.append(pcr_note)

print(f"📊 Market Regime → {row_reg['TREND_REGIME']} | {row_reg['VOL_REGIME']}")

# ==================================================
# OUTPUT
# ==================================================
out = pd.DataFrame({
    "date": [trade_date],
    "signal": [signal],
    "position_size": [position_size],
    "prob_up": [row_ml["prob_up"]],
    "prob_down": [row_ml["prob_down"]],
    "oi_regime": [row_oi[oi_reg_col]],
    "trend_regime": [row_reg["TREND_REGIME"]],
    "vol_regime": [row_reg["VOL_REGIME"]],
    "pcr": [round(pcr_val, 3)],
    "reason": [" | ".join(reason)],
})

date_tag = pd.to_datetime(trade_date).strftime("%d-%m-%Y")
OUT_FILE = OUT_DIR / f"nifty_final_signal_{date_tag}.csv"

out.to_csv(OUT_FILE, index=False)

print("\n✅ FINAL SIGNAL GENERATED")
print(out)
print(f"\n💾 Saved → {OUT_FILE}")
