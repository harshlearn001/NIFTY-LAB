#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | CALIBRATED ENSEMBLE INFERENCE
XGB (Temperature) + LGBM (Isotonic)
"""

import joblib
import numpy as np
import pandas as pd
from configs.paths import BASE_DIR

print("🚀 CALIBRATED ENSEMBLE INFERENCE")

# --------------------------------------------------
# PATHS
# --------------------------------------------------
FEAT_FILE = BASE_DIR / "data/processed/ml/nifty_inference_features.parquet"

XGB_MODEL = BASE_DIR / "models/nifty_xgb_gpu.joblib"
XGB_TEMP  = BASE_DIR / "models/nifty_xgb_temperature.txt"

LGBM_MODEL = BASE_DIR / "models/nifty_lgbm_model.joblib"
LGBM_CAL   = BASE_DIR / "models/nifty_lgbm_calibrated.joblib"

OUT_FILE  = BASE_DIR / "data/processed/ml/nifty_ml_prediction.parquet"

# --------------------------------------------------
# LOAD FEATURES
# --------------------------------------------------
X = pd.read_parquet(FEAT_FILE)
date = X["date"].iloc[0]

# --------------------------------------------------
# XGB PREDICTION (TEMPERATURE SCALED)
# --------------------------------------------------
xgb = joblib.load(XGB_MODEL)
temperature = float(XGB_TEMP.read_text().strip())

xgb_feats = list(xgb.feature_names_in_)
X_xgb = X.copy()
for c in xgb_feats:
    if c not in X_xgb.columns:
        X_xgb[c] = 0.0
X_xgb = X_xgb[xgb_feats]

raw = xgb.predict_proba(X_xgb)[0, 1]

# Temperature scaling (logit space)
logit = np.log(raw / (1 - raw))
xgb_cal = 1 / (1 + np.exp(-logit / temperature))

# --------------------------------------------------
# LGBM PREDICTION (ISOTONIC)
# --------------------------------------------------
lgbm = joblib.load(LGBM_MODEL)
iso  = joblib.load(LGBM_CAL)

lgbm_feats = list(lgbm.feature_name_)
X_lgbm = X.copy()
for c in lgbm_feats:
    if c not in X_lgbm.columns:
        X_lgbm[c] = 0.0
X_lgbm = X_lgbm[lgbm_feats]

lgbm_raw = lgbm.predict_proba(X_lgbm)[0, 1]
lgbm_cal = iso.predict([lgbm_raw])[0]

# --------------------------------------------------
# ENSEMBLE
# --------------------------------------------------
prob_up = round((xgb_cal + lgbm_cal) / 2, 5)
prob_down = round(1 - prob_up, 5)

out = pd.DataFrame({
    "date": [date],
    "prob_up": [prob_up],
    "prob_down": [prob_down],
    "xgb_cal": [round(xgb_cal, 5)],
    "lgbm_cal": [round(lgbm_cal, 5)],
})

out.to_parquet(OUT_FILE, index=False)

print("\n✅ CALIBRATED ENSEMBLE READY")
print(out)
print(f"💾 Saved → {OUT_FILE}")
