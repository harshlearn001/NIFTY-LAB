#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | LIGHTGBM TRAINING (PRODUCTION SAFE)

✔ Drop-in replacement for XGBoost
✔ Uses canonical ML features
✔ Time-safe split
✔ No API issues
"""

import pandas as pd
import lightgbm as lgb
import joblib
from pathlib import Path

from configs.paths import BASE_DIR

print("🚀 TRAINING LIGHTGBM (NIFTY)")

# --------------------------------------------------
# PATHS
# --------------------------------------------------
DATA_FILE = BASE_DIR / "data/processed/ml/nifty_ml_features_CANONICAL.parquet"
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = MODEL_DIR / "nifty_lgbm_model.joblib"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
df = pd.read_parquet(DATA_FILE).dropna()

TARGET = "target"
DROP_COLS = ["date", "next_ret", TARGET]

X = df.drop(columns=DROP_COLS)
y = df[TARGET].astype(int)

# --------------------------------------------------
# TIME-BASED SPLIT
# --------------------------------------------------
split = int(len(df) * 0.8)

X_train, X_val = X.iloc[:split], X.iloc[split:]
y_train, y_val = y.iloc[:split], y.iloc[split:]

# --------------------------------------------------
# MODEL
# --------------------------------------------------
model = lgb.LGBMClassifier(
    objective="binary",
    n_estimators=600,
    learning_rate=0.03,
    num_leaves=64,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.5,
    reg_lambda=0.5,
    random_state=42,
)

# --------------------------------------------------
# TRAIN (LightGBM SAFE)
# --------------------------------------------------
model.fit(
    X_train,
    y_train,
    eval_set=[(X_val, y_val)],
    eval_metric="auc",
    callbacks=[
        lgb.log_evaluation(period=50),
        lgb.early_stopping(stopping_rounds=50)
    ],
)

# --------------------------------------------------
# SAVE MODEL
# --------------------------------------------------
joblib.dump(model, MODEL_FILE)

print("✅ LIGHTGBM MODEL TRAINED")
print(f"💾 Saved → {MODEL_FILE}")

# --------------------------------------------------
# FEATURE IMPORTANCE
# --------------------------------------------------
imp = pd.Series(model.feature_importances_, index=X.columns)
print("\n🔍 TOP FEATURES")
print(imp.sort_values(ascending=False).head(12))
