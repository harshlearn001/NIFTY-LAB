#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Train GPU XGBoost Model for NIFTY
================================

Predicts probability of profitable next-day move.
Uses RTX 3080 Ti (gpu_hist).

Output:
- Trained model
- Daily predictions with BUY/SELL + SL + TARGET
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, roc_auc_score
from configs.paths import PROC_DIR

# =================================================
# PATHS
# =================================================
DATA_FILE = PROC_DIR / "ml" / "nifty_ml_features.parquet"
MODEL_DIR = PROC_DIR / "ml" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MODEL_FILE = MODEL_DIR / "xgb_nifty_gpu.json"
PRED_FILE  = PROC_DIR / "ml" / "nifty_ml_predictions.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading ML feature dataset...")
df = pd.read_parquet(DATA_FILE).sort_values("date").reset_index(drop=True)

# =================================================
# FEATURES & TARGET
# =================================================
TARGET = "target"
FEATURES = [c for c in df.columns if c not in [
    "date", "close", "next_close", "next_ret", "target"
]]

X = df[FEATURES]
y = df[TARGET]

# =================================================
# TRAIN / TEST SPLIT (TIME-AWARE)
# =================================================
split = int(len(df) * 0.75)

X_train, X_test = X.iloc[:split], X.iloc[split:]
y_train, y_test = y.iloc[:split], y.iloc[split:]

# =================================================
# GPU XGBOOST MODEL
# =================================================
model = XGBClassifier(
    tree_method="hist",
    device="cuda",
    max_depth=4,
    n_estimators=500,
    learning_rate=0.04,
    subsample=0.8,
    colsample_bytree=0.8,
    eval_metric="logloss",
    random_state=42,
)


print("🚀 Training GPU XGBoost model...")
model.fit(X_train, y_train)

# =================================================
# EVALUATION
# =================================================
probs = model.predict_proba(X_test)[:, 1]
preds = (probs > 0.5).astype(int)

print("\n📊 MODEL PERFORMANCE")
print(classification_report(y_test, preds))
print("ROC-AUC:", round(roc_auc_score(y_test, probs), 3))

# =================================================
# SAVE MODEL
# =================================================
model.save_model(MODEL_FILE)
print(f"\n💾 Model saved: {MODEL_FILE}")

# =================================================
# FULL DATA PREDICTIONS
# =================================================
df["ml_prob"] = model.predict_proba(X)[:, 1]

# =================================================
# TRADE ENGINE (FINAL OUTPUT)
# =================================================
ATR_MULT_SL = 1.0
ATR_MULT_TGT = 2.0
PROB_THRESHOLD = 0.60

df["signal"] = "NO_TRADE"

df.loc[df["ml_prob"] >= PROB_THRESHOLD, "signal"] = "BUY"
df.loc[df["ml_prob"] <= (1 - PROB_THRESHOLD), "signal"] = "SELL"

# SL / TARGET
df["stop_loss"] = np.nan
df["target"] = np.nan

df.loc[df["signal"] == "BUY", "stop_loss"] = (
    df["close"] - ATR_MULT_SL * df["atr_pct"] * df["close"]
)
df.loc[df["signal"] == "BUY", "target"] = (
    df["close"] + ATR_MULT_TGT * df["atr_pct"] * df["close"]
)

df.loc[df["signal"] == "SELL", "stop_loss"] = (
    df["close"] + ATR_MULT_SL * df["atr_pct"] * df["close"]
)
df.loc[df["signal"] == "SELL", "target"] = (
    df["close"] - ATR_MULT_TGT * df["atr_pct"] * df["close"]
)

# =================================================
# SAVE PREDICTIONS
# =================================================
out_cols = [
    "date",
    "signal",
    "close",
    "stop_loss",
    "target",
    "ml_prob",
]

df[out_cols].to_parquet(PRED_FILE, index=False)

print("\n✅ FINAL ML TRADE SHEET GENERATED")
print(f"📦 File: {PRED_FILE}")
print("\nSample:")
print(df[out_cols].tail(5))
