#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | XGBOOST INFERENCE (PRODUCTION)

✔ Uses inference features
✔ Safe fallback if model missing
✔ Writes ML prediction for final strategy
"""

from pathlib import Path
import pandas as pd
import joblib

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

FEATURE_FILE = BASE / "data" / "processed" / "ml" / "nifty_inference_features.parquet"
MODEL_FILE   = BASE / "models" / "nifty_xgb_model.pkl"

OUT_DIR  = BASE / "data" / "processed" / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_ml_prediction.parquet"

print("NIFTY-LAB | XGBOOST INFERENCE")
print("-" * 60)

# --------------------------------------------------
# LOAD FEATURES
# --------------------------------------------------
if not FEATURE_FILE.exists():
    print("❌ Inference features not found. Skipping inference.")
    exit(0)

X = pd.read_parquet(FEATURE_FILE)

print("📥 Loaded inference features")
print(X)

# --------------------------------------------------
# LOAD MODEL (SAFE)
# --------------------------------------------------
if not MODEL_FILE.exists():
    print("⚠️ Model not found — using neutral probabilities")

    prob_up = 0.5
    prob_down = 0.5

else:
    model = joblib.load(MODEL_FILE)

    # Ensure correct feature order
    feature_cols = model.feature_names_in_
    X_model = X[feature_cols]

    probs = model.predict_proba(X_model)[0]
    prob_down = probs[0]
    prob_up   = probs[1]

# --------------------------------------------------
# OUTPUT
# --------------------------------------------------
out = pd.DataFrame({
    "date": X["date"],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
})

out.to_parquet(OUT_FILE, index=False)

print("\n✅ ML INFERENCE COMPLETE")
print(out)
print(f"💾 Saved : {OUT_FILE}")
