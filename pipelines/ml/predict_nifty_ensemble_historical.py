#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ENSEMBLE PREDICTION (XGBOOST SAFE)

✔ Exact feature name match
✔ Exact feature order match
✔ Case sensitive safe
✔ CSV + Parquet output
✔ Production ready
"""

import sys
from pathlib import Path
import pandas as pd
import joblib

# ==================================================
# BOOTSTRAP
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import PROC_DIR, MODEL_DIR

# ==================================================
# PATHS
# ==================================================
DATA_FILE = PROC_DIR / "ml" / "nifty_ml_features_hist_no_pcr.csv"

OUT_DIR = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "nifty_ml_prediction.csv"
OUT_PQ  = OUT_DIR / "nifty_ml_prediction.parquet"

# ==================================================
# LOAD DATA
# ==================================================
print("📥 Loading historical ML features...")
df = pd.read_csv(DATA_FILE)

# normalize column names
df.columns = df.columns.str.lower()

# ==================================================
# 🔒 RENAME → EXACT TRAINING SCHEMA
# ==================================================
df = df.rename(columns={
    "regime_long_buildup": "regime_LONG_BUILDUP",
    "regime_short_covering": "regime_SHORT_COVERING",
    "regime_no_data": "regime_NO_DATA",
})

# ==================================================
# 🔒 EXACT FEATURE ORDER (THIS IS THE KEY FIX)
# ==================================================
FEATURES = [
    "close",
    "ret_1d",
    "ret_3d",
    "atr_pct",
    "trend_up",
    "oi_change_pct",
    "regime_LONG_BUILDUP",
    "regime_NO_DATA",            # ⚠ ORDER MATTERS
    "regime_SHORT_COVERING",     # ⚠ ORDER MATTERS
]

missing = set(FEATURES) - set(df.columns)
if missing:
    raise RuntimeError(f"❌ Missing required features: {missing}")

# FORCE ORDER
X = df.loc[:, FEATURES].copy()

# ==================================================
# LOAD MODELS
# ==================================================
print("📦 Loading models...")

xgb = joblib.load(MODEL_DIR / "nifty_xgb_gpu.joblib")

lgbm_path = MODEL_DIR / "nifty_lgbm.joblib"
lgbm = joblib.load(lgbm_path) if lgbm_path.exists() else None

# ==================================================
# PREDICT
# ==================================================
print("🤖 Predicting historical probabilities...")

p_xgb = xgb.predict_proba(X)[:, 1]

if lgbm:
    p_lgbm = lgbm.predict_proba(X)[:, 1]
    p_ens = (p_xgb + p_lgbm) / 2
else:
    p_ens = p_xgb

# ==================================================
# OUTPUT
# ==================================================
out = pd.DataFrame({
    "DATE": pd.to_datetime(df["date"]),
    "PROB_UP": p_ens,
    "PROB_DOWN": 1 - p_ens,
})

out.to_csv(OUT_CSV, index=False)
out.to_parquet(OUT_PQ, index=False)

# ==================================================
# SUMMARY
# ==================================================
print("\n✅ HISTORICAL ENSEMBLE PREDICTIONS READY")
print(f"📦 CSV     : {OUT_CSV}")
print(f"📦 Parquet : {OUT_PQ}")
print(f"📊 Rows    : {len(out):,}")
print("\nSample:")
print(out.head(3))
