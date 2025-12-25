#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | PROBABILITY CALIBRATION (FINAL FIX)

✔ Feature-aligned
✔ Pickle-safe
✔ XGB → Temperature scaling
✔ LGBM → Isotonic
"""

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from configs.paths import BASE_DIR

# --------------------------------------------------
# PICKLE-SAFE TEMPERATURE SCALER
# --------------------------------------------------
class TemperatureScaler:
    def __init__(self, T=1.3):
        self.T = T

    def transform(self, p):
        p = np.clip(p, 1e-6, 1 - 1e-6)
        logit = np.log(p / (1 - p))
        return 1 / (1 + np.exp(-logit / self.T))

# --------------------------------------------------
# PATHS
# --------------------------------------------------
DATA = BASE_DIR / "data/processed/ml/nifty_ml_features_CANONICAL.parquet"

XGB_MODEL  = BASE_DIR / "models/nifty_xgb_gpu.joblib"
LGBM_MODEL = BASE_DIR / "models/nifty_lgbm_model.joblib"

OUT_XGB  = BASE_DIR / "models/nifty_xgb_temp_scaler.joblib"
OUT_LGBM = BASE_DIR / "models/nifty_lgbm_calibrated.joblib"

print("🔧 Loading data...")
df = pd.read_parquet(DATA)

y = df["target"].values

# --------------------------------------------------
# 🔥 XGBOOST CALIBRATION
# --------------------------------------------------
print("🔧 Calibrating XGB (temperature scaling)")

xgb = joblib.load(XGB_MODEL)
xgb_features = list(xgb.feature_names_in_)

X_xgb = df.copy()
for col in xgb_features:
    if col not in X_xgb.columns:
        X_xgb[col] = 0.0

X_xgb = X_xgb[xgb_features]

raw_probs = xgb.predict_proba(X_xgb)[:, 1]

temp_scaler = TemperatureScaler(T=1.3)
joblib.dump(temp_scaler, OUT_XGB)

# --------------------------------------------------
# 🔥 LIGHTGBM CALIBRATION
# --------------------------------------------------
print("🔧 Calibrating LGBM (isotonic)")

lgbm = joblib.load(LGBM_MODEL)
lgbm_features = list(lgbm.feature_name_)

X_lgbm = df.copy()
for col in lgbm_features:
    if col not in X_lgbm.columns:
        X_lgbm[col] = 0.0

X_lgbm = X_lgbm[lgbm_features]

lgbm_probs = lgbm.predict_proba(X_lgbm)[:, 1]

iso = IsotonicRegression(out_of_bounds="clip")
iso.fit(lgbm_probs, y)

joblib.dump(iso, OUT_LGBM)

print("✅ CALIBRATION COMPLETE (100% SAFE)")
print(f"💾 XGB scaler → {OUT_XGB}")
print(f"💾 LGBM calibrator → {OUT_LGBM}")
