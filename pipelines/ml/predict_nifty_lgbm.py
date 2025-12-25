#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | LIGHTGBM INFERENCE (LIVE + BACKTEST)

✔ Uses inference features
✔ Schema-safe
✔ Writes nifty_ml_prediction.parquet
"""

import pandas as pd
import joblib
from pathlib import Path

from configs.paths import BASE_DIR

print("📈 LIGHTGBM INFERENCE")

# --------------------------------------------------
# PATHS
# --------------------------------------------------
FEATURE_FILE = BASE_DIR / "data/processed/ml/nifty_inference_features.parquet"
MODEL_FILE   = BASE_DIR / "models/nifty_lgbm_model.joblib"
OUT_FILE     = BASE_DIR / "data/processed/ml/nifty_ml_prediction.parquet"

# --------------------------------------------------
# LOAD FEATURES
# --------------------------------------------------
if not FEATURE_FILE.exists():
    print("❌ Inference features missing")
    exit(0)

X = pd.read_parquet(FEATURE_FILE)

# --------------------------------------------------
# LOAD MODEL
# --------------------------------------------------
if not MODEL_FILE.exists():
    print("❌ Model missing — neutral output")
    prob_up, prob_down = 0.5, 0.5
else:
    model = joblib.load(MODEL_FILE)
    feature_cols = model.feature_name_

    X_aligned = X.copy()
    for col in feature_cols:
        if col not in X_aligned.columns:
            X_aligned[col] = 0.0

    X_model = X_aligned[feature_cols]
    prob_up = float(model.predict_proba(X_model)[0][1])
    prob_down = 1.0 - prob_up

# --------------------------------------------------
# SAVE
# --------------------------------------------------
out = pd.DataFrame({
    "date": X["date"],
    "prob_up": [round(prob_up, 5)],
    "prob_down": [round(prob_down, 5)],
})

out.to_parquet(OUT_FILE, index=False)

print("✅ LIGHTGBM INFERENCE COMPLETE")
print(out)
print(f"💾 Saved → {OUT_FILE}")
