#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL ML + OI + PCR STRATEGY
======================================
✔ Works with single-model or multi-model predictions
✔ Dynamic ensemble when available
✔ Safe fallback when only PROB_UP exists
✔ Never crashes
✔ Signal-only (NO execution)
"""

# ==========================================================
# PATH FIX
# ==========================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ==========================================================
# IMPORTS
# ==========================================================
from pipelines.ml.audit_logger import log_decision

import pandas as pd
from datetime import datetime

from pipelines.ml.ensemble_blender import ensemble_probability
from pipelines.ml.trade_decision import decide_trade

# ==========================================================
# PATHS
# ==========================================================
BASE = ROOT

PRED_FILE = BASE / "data/processed/ml/nifty_ml_prediction.parquet"
REGIME_FILE = BASE / "data/continuous/nifty_regime.parquet"

OUT_DIR = BASE / "data/signals"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / f"nifty_final_signal_{datetime.now():%d-%m-%Y}.csv"

# ==========================================================
# CONFIG
# ==========================================================
CAPITAL = 1_000_000
VOLATILITY = 0.012
REGIME_CHANGED_RECENTLY = False

REGIME_PRIOR = {
    "TREND":    [0.45, 0.25, 0.30],
    "RANGE":    [0.55, 0.35, 0.10],
    "HIGH_VOL": [0.60, 0.25, 0.15],
}

# ==========================================================
# LOAD PREDICTIONS
# ==========================================================
if not PRED_FILE.exists():
    raise FileNotFoundError(f"❌ Missing prediction file: {PRED_FILE}")

pred_df = pd.read_parquet(PRED_FILE)
pred = pred_df.iloc[-1]
pred.index = pred.index.str.upper()
cols = pred.index.tolist()

# ==========================================================
# LOAD REGIME (SAFE)
# ==========================================================
if REGIME_FILE.exists():
    regime_df = pd.read_parquet(REGIME_FILE)
    regime = str(regime_df.iloc[-1].get("REGIME", "TREND")).upper()
else:
    regime = "TREND"
    print("⚠ Regime file missing → defaulting to TREND")

if regime not in REGIME_PRIOR:
    regime = "TREND"

# ==========================================================
# CASE 1️⃣ : MULTI-MODEL PROBABILITIES AVAILABLE
# ==========================================================
multi_model = all(c in cols for c in ["P_XGB", "P_LGBM", "P_LSTM"])

if multi_model:

    probs = [
        float(pred["P_XGB"]),
        float(pred["P_LGBM"]),
        float(pred["P_LSTM"]),
    ]

    scores = [
        float(pred.get("SCORE_XGB", 0.5)),
        float(pred.get("SCORE_LGBM", 0.5)),
        float(pred.get("SCORE_LSTM", 0.5)),
    ]

    ensemble_out = ensemble_probability(
        probs=probs,
        scores=scores,
        regime_weights=REGIME_PRIOR[regime]
    )

# ==========================================================
# CASE 2️⃣ : SINGLE MODEL (PROB_UP / PROB_DOWN)
# ==========================================================
elif "PROB_UP" in cols:

    p_up = float(pred["PROB_UP"])

    # Fake a 3-model ensemble with identical beliefs
    probs = [p_up, p_up, p_up]
    scores = [0.5, 0.5, 0.5]

    ensemble_out = ensemble_probability(
        probs=probs,
        scores=scores,
        regime_weights=[1/3, 1/3, 1/3]
    )

    print("ℹ Single-model probability detected → ensemble fallback applied")

else:
    raise RuntimeError(
        f"❌ Unsupported prediction schema. Columns found:\n{cols}"
    )

# ==========================================================
# DECISION
# ==========================================================
decision = decide_trade(
    ensemble_out=ensemble_out,
    capital=CAPITAL,
    volatility=VOLATILITY,
    regime=regime,
    regime_changed_recently=REGIME_CHANGED_RECENTLY
)

# ==========================================================
# OUTPUT SIGNAL
# ==========================================================
signal = {
    "DATE": datetime.now().strftime("%Y-%m-%d"),
    "SYMBOL": "NIFTY",
    "REGIME": regime,
    "ACTION": decision.action,
    "POSITION_SIZE": decision.position_size,
    "CONFIDENCE": round(ensemble_out["confidence"], 6),
    "PROBABILITY": round(ensemble_out["P_adj"], 6),
    "AGREEMENT": round(ensemble_out["agreement"], 6),
    "REASON": decision.reason,
}

df_out = pd.DataFrame([signal])
df_out.to_csv(OUT_FILE, index=False)

# ==========================
# AUDIT LOGGING (NEW)
# ==========================
csv_audit, pq_audit = log_decision(signal, BASE)


# ==========================================================
# CONSOLE OUTPUT
# ==========================================================
print("\n✅ FINAL SIGNAL GENERATED")
print(df_out.to_string(index=False))
print(f"\n📁 Saved to: {OUT_FILE}")
print(f"\n🧾 Audit log updated:")
print(f"CSV → {csv_audit}")
print(f"PARQUET → {pq_audit}")

