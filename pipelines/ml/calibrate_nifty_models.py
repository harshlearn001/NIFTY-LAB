#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | XGBOOST PROBABILITY CALIBRATION (TEMPERATURE SCALING)

✔ Uses historical ML dataset
✔ Enforces exact XGB feature order
✔ No leakage
✔ Pickle-safe
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import joblib

# --------------------------------------------------
# PROJECT ROOT
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import PROC_DIR
from pipelines.ml.temperature_scaler import TemperatureScaler

# --------------------------------------------------
# PATHS
# --------------------------------------------------
DATA_FILE = PROC_DIR / "ml" / "nifty_ml_features_hist_no_pcr.parquet"

MODEL_DIR = ROOT / "models"
XGB_MODEL = MODEL_DIR / "nifty_xgb_gpu.joblib"
OUT_SCALER = MODEL_DIR / "nifty_xgb_temp_scaler.joblib"
OUT_TEMP = MODEL_DIR / "nifty_xgb_temperature.txt"

# --------------------------------------------------
def main():
    print("📦 Loading historical ML dataset...")
    df = pd.read_parquet(DATA_FILE)
    print(f"📊 Samples: {len(df):,}")

    y = df["target"].values

    print("🤖 Loading XGBoost model...")
    model = joblib.load(XGB_MODEL)

    # --------------------------------------------------
    # FEATURE ALIGNMENT (CRITICAL)
    # --------------------------------------------------
    features = model.get_booster().feature_names
    print(f"🧠 Features used ({len(features)}):")
    print(features)

    X = df[features]

    print("🔥 Generating raw probabilities...")
    raw_prob = model.predict_proba(X)[:, 1]

    eps = 1e-6
    logits = np.log((raw_prob + eps) / (1 - raw_prob + eps))

    print("🌡 Calibrating with temperature scaling...")
    scaler = TemperatureScaler().fit(logits, y)

    joblib.dump(scaler, OUT_SCALER)
    OUT_TEMP.write_text(f"{scaler.temperature_:.6f}")

    print("\n✅ MODEL CALIBRATION COMPLETE")
    print(f"💾 Scaler saved → {OUT_SCALER}")
    print(f"🌡 Temperature → {scaler.temperature_:.4f}")


if __name__ == "__main__":
    main()
