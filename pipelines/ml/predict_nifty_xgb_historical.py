#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ML PREDICTION (CANONICAL SAFE)

✔ Uses CANONICAL features
✔ Strict feature alignment
✔ No leakage
✔ Backtest ready
"""

import joblib
import pandas as pd
from pathlib import Path

from configs.paths import BASE_DIR

print("🚀 Building historical ML predictions (canonical aligned)")

# --------------------------------------------------
# PATHS
# --------------------------------------------------
FEATURE_FILE = BASE_DIR / "data/processed/ml/nifty_ml_features_CANONICAL.parquet"
MODEL_FILE   = BASE_DIR / "models/nifty_xgb_gpu.joblib"
OUT_FILE     = BASE_DIR / "data/processed/ml/nifty_ml_prediction_historical.parquet"

# --------------------------------------------------
# LOAD
# --------------------------------------------------
if not FEATURE_FILE.exists():
    raise FileNotFoundError(f"Missing features: {FEATURE_FILE}")

if not MODEL_FILE.exists():
    raise FileNotFoundError(f"Missing model: {MODEL_FILE}")

df = pd.read_parquet(FEATURE_FILE)
model = joblib.load(MODEL_FILE)

# --------------------------------------------------
# FEATURE ALIGNMENT (CRITICAL)
# --------------------------------------------------
feature_names = model.get_booster().feature_names
print(f"✔ Model expects {len(feature_names)} features")

X = df[feature_names].copy()

# --------------------------------------------------
# PREDICT
# --------------------------------------------------
proba = model.predict_proba(X)

df_out = pd.DataFrame({
    "date": df["date"],
    "prob_down": proba[:, 0],
    "prob_up": proba[:, 1],
})

# --------------------------------------------------
# SAVE
# --------------------------------------------------
df_out.to_parquet(OUT_FILE, index=False)

print("✅ HISTORICAL ML PREDICTIONS GENERATED")
print(df_out.tail())
print(f"💾 Saved → {OUT_FILE}")
