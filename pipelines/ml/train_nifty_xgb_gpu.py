#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | XGBOOST GPU TRAINING (HISTORICAL)

✔ Uses full historical ML dataset
✔ PCR + OI + Regime aware
✔ Time-safe split
✔ RTX 3080 Ti (CUDA)
✔ Production ready
"""

from pathlib import Path
import pandas as pd
import joblib
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DATA_FILE = BASE / "data" / "processed" / "ml" / "nifty_ml_features_train.parquet"

MODEL_DIR = BASE / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = MODEL_DIR / "nifty_xgb_gpu.joblib"

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
print("📥 Loading historical ML dataset...")

df = pd.read_parquet(DATA_FILE)
print(f"📊 Total rows : {len(df):,}")

# --------------------------------------------------
# FEATURES / TARGET
# --------------------------------------------------
TARGET = "target"
DROP_COLS = ["date", "next_close", "next_ret", TARGET]

X = df.drop(columns=DROP_COLS)
y = df[TARGET].astype(int)

# --------------------------------------------------
# TIME-SAFE SPLIT (80 / 20)
# --------------------------------------------------
split = int(len(df) * 0.8)

X_train, X_val = X.iloc[:split], X.iloc[split:]
y_train, y_val = y.iloc[:split], y.iloc[split:]

print(f"Train rows : {len(X_train):,}")
print(f"Val rows   : {len(X_val):,}")

# --------------------------------------------------
# GPU MODEL (XGBOOST ≥ 2.0)
# --------------------------------------------------
print("\n🚀 Training XGBoost on RTX 3080 Ti...")

model = XGBClassifier(
    n_estimators=1200,
    max_depth=6,
    learning_rate=0.03,
    subsample=0.85,
    colsample_bytree=0.85,
    min_child_weight=3,
    gamma=0.2,
    reg_lambda=1.5,
    objective="binary:logistic",
    eval_metric="logloss",
    tree_method="hist",
    device="cuda",
    random_state=42,
)

model.fit(X_train, y_train)

# --------------------------------------------------
# VALIDATION
# --------------------------------------------------
val_pred = model.predict(X_val)
val_prob = model.predict_proba(X_val)[:, 1]

acc = accuracy_score(y_val, val_pred)
auc = roc_auc_score(y_val, val_prob)

print("\n📈 VALIDATION METRICS")
print(f"Accuracy : {acc:.4f}")
print(f"AUC      : {auc:.4f}")

# --------------------------------------------------
# SAVE MODEL
# --------------------------------------------------
joblib.dump(model, MODEL_FILE)

print("\n✅ MODEL TRAINED & SAVED")
print(f"💾 Model : {MODEL_FILE}")

# --------------------------------------------------
# FEATURE IMPORTANCE
# --------------------------------------------------
imp = (
    pd.Series(model.feature_importances_, index=X.columns)
      .sort_values(ascending=False)
)

print("\n🔍 TOP FEATURES")
print(imp.head(12))
