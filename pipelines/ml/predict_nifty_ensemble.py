#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | DAILY XGBOOST ENSEMBLE PREDICTION (PRODUCTION SAFE)

✔ Exact feature alignment with training
✔ Auto-fills missing one-hot regime columns
✔ Drops extra columns safely
✔ Uses calibrated probabilities
✔ Backtest = Daily = Live compatible
"""

import sys
from pathlib import Path
import pandas as pd
import joblib
import warnings
import numpy as np

# ==================================================
# PROJECT ROOT (CRITICAL)
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import PROC_DIR

# ==================================================
# PATHS
# ==================================================
FEAT_FILE = PROC_DIR / "ml" / "nifty_inference_features.parquet"

MODEL_DIR = ROOT / "models"
XGB_MODEL = MODEL_DIR / "nifty_xgb_gpu.joblib"
CALIB    = MODEL_DIR / "nifty_xgb_temp_scaler.joblib"

OUT_DIR = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "nifty_ml_daily_prediction.csv"
OUT_PQ  = OUT_DIR / "nifty_ml_daily_prediction.parquet"

# ==================================================
# LOAD INFERENCE FEATURES
# ==================================================
print("📥 Loading inference features...")
df = pd.read_parquet(FEAT_FILE)

if "date" not in df.columns:
    raise RuntimeError("❌ 'date' column missing in inference features")

dates = df["date"].copy()
X_raw = df.drop(columns=["date"], errors="ignore")

# ==================================================
# LOAD MODEL & CALIBRATOR
# ==================================================
print("📦 Loading XGBoost model & calibrator...")
xgb = joblib.load(XGB_MODEL)
cal = joblib.load(CALIB)

# ==================================================
# ENFORCE EXACT FEATURE SET (CRITICAL FIX)
# ==================================================
expected_features = xgb.get_booster().feature_names

print(f"🧠 Features used ({len(expected_features)}):")
print(expected_features)

# --------------------------------------------------
# FIX MISSING / EXTRA FEATURES
# --------------------------------------------------
missing = set(expected_features) - set(X_raw.columns)
extra   = set(X_raw.columns) - set(expected_features)

# ➕ Add missing one-hot columns as ZERO
for col in missing:
    X_raw[col] = 0.0

# ➖ Drop extra columns
if extra:
    X_raw = X_raw.drop(columns=list(extra))

# 🔁 Enforce strict column order
X = X_raw[expected_features]

# ==================================================
# PREDICT (SAFE)
# ==================================================
print("🤖 Predicting daily probabilities...")

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    raw_prob = xgb.predict_proba(X)[:, 1]

# ==================================================
# CALIBRATION (LOGITS → PROB)
# ==================================================
eps = 1e-6
logits = np.log((raw_prob + eps) / (1 - raw_prob + eps))
prob_up = cal.transform(logits).ravel()

# ==================================================
# OUTPUT
# ==================================================
out = pd.DataFrame({
    "DATE": dates,
    "PROB_UP": prob_up,
    "PROB_DOWN": 1.0 - prob_up
})

out.to_csv(OUT_CSV, index=False)
out.to_parquet(OUT_PQ, index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n✅ DAILY XGBOOST PREDICTION READY")
print(f"📦 CSV     : {OUT_CSV}")
print(f"📦 Parquet : {OUT_PQ}")
print("\nSignal:")
print(out.tail(1).T)
