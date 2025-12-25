#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | XGBOOST INFERENCE (PRODUCTION)

✔ Uses inference features
✔ Loads trained GPU XGB model
✔ Feature-aligned prediction
✔ BASE_DIR safe
"""

import pandas as pd
import joblib
from configs.paths import BASE_DIR

# --------------------------------------------------
# PATHS
# --------------------------------------------------
FEATURE_FILE = BASE_DIR / "data/processed/ml/nifty_inference_features.parquet"
MODEL_FILE   = BASE_DIR / "models/nifty_xgb_gpu.joblib"
OUT_FILE     = BASE_DIR / "data/processed/ml/nifty_ml_prediction.parquet"

print("NIFTY-LAB | XGBOOST INFERENCE")
print("-" * 60)

# --------------------------------------------------
# LOAD FEATURES
# --------------------------------------------------
if not FEATURE_FILE.exists():
    print("❌ Inference features not found")
    exit(0)

X = pd.read_parquet(FEATURE_FILE)
print("Loaded inference features")
print(X)

# --------------------------------------------------
# LOAD MODEL
# --------------------------------------------------
if not MODEL_FILE.exists():
    print("❌ Model not found — using neutral probabilities")
    prob_up, prob_down = 0.5, 0.5

else:
    model = joblib.load(MODEL_FILE)
    print(f"Loaded model : {MODEL_FILE.name}")

    feature_cols = list(model.feature_names_in_)
    X_aligned = X.copy()

    for col in feature_cols:
        if col not in X_aligned.columns:
            X_aligned[col] = 0.0

    X_model = X_aligned[feature_cols]
    probs = model.predict_proba(X_model)[0]

    prob_down = float(probs[0])
    prob_up   = float(probs[1])

# --------------------------------------------------
# SAVE OUTPUT
# --------------------------------------------------
out = pd.DataFrame({
    "date": X["date"],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
})

out.to_parquet(OUT_FILE, index=False)

print("\n✅ ML INFERENCE COMPLETE")
print(out)
print(f"💾 Saved → {OUT_FILE}")
