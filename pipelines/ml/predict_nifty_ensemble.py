#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | ML ENSEMBLE INFERENCE (XGB + LGBM)

✔ Production safe
✔ Uses SAME inference features
✔ Probability ensemble
✔ Drop-in replacement for final strategy
"""

import pandas as pd
import joblib
from pathlib import Path

# --------------------------------------------------
# PATHS
# --------------------------------------------------
from configs.paths import BASE_DIR

FEATURE_FILE = BASE_DIR / "data/processed/ml/nifty_inference_features.parquet"

XGB_MODEL = BASE_DIR / "models/nifty_xgb_gpu.joblib"
LGBM_MODEL = BASE_DIR / "models/nifty_lgbm_model.joblib"

OUT_DIR = BASE_DIR / "data/processed/ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_ml_prediction.parquet"

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
W_XGB = 0.5
W_LGBM = 0.5

print("🚀 NIFTY ML ENSEMBLE INFERENCE (XGB + LGBM)")
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
# SAFE MODEL LOAD
# --------------------------------------------------
def predict_safe(model_path, X):
    if not model_path.exists():
        print(f"⚠️ Missing model → {model_path.name} (neutral)")
        return 0.5, 0.5

    model = joblib.load(model_path)
    features = list(model.feature_names_in_)

    X_aligned = X.copy()
    for col in features:
        if col not in X_aligned.columns:
            X_aligned[col] = 0

    X_model = X_aligned[features]
    probs = model.predict_proba(X_model)[0]

    return float(probs[1]), float(probs[0])  # up, down


# --------------------------------------------------
# PREDICTIONS
# --------------------------------------------------
xgb_up, xgb_down = predict_safe(XGB_MODEL, X)
lgbm_up, lgbm_down = predict_safe(LGBM_MODEL, X)

# --------------------------------------------------
# ENSEMBLE
# --------------------------------------------------
prob_up = (W_XGB * xgb_up) + (W_LGBM * lgbm_up)
prob_down = 1.0 - prob_up

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
out = pd.DataFrame({
    "date": X["date"],
    "prob_up": [round(prob_up, 5)],
    "prob_down": [round(prob_down, 5)],
    "xgb_up": [round(xgb_up, 5)],
    "lgbm_up": [round(lgbm_up, 5)],
})

out.to_parquet(OUT_FILE, index=False)

print("\n✅ ENSEMBLE INFERENCE COMPLETE")
print(out)
print(f"\n💾 Saved → {OUT_FILE}")
