#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | XGBOOST INFERENCE (EOD)

✔ Loads trained GPU XGBoost model
✔ Uses inference-only features (no leakage)
✔ Outputs probability for next day move
✔ Scheduler safe
"""

from pathlib import Path
import joblib
import pandas as pd
import numpy as np

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

MODEL_FILE = BASE / "models" / "nifty_xgb_gpu.joblib"
INF_FILE   = BASE / "data" / "processed" / "ml" / "nifty_ml_inference.parquet"

OUT_DIR = BASE / "data" / "processed" / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_ml_prediction.parquet"
OUT_CSV = OUT_DIR / "nifty_ml_prediction.csv"


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("NIFTY-LAB | XGBOOST INFERENCE")
    print("-" * 60)

    if not MODEL_FILE.exists():
        print("❌ Model not found. Skipping inference.")
        return

    if not INF_FILE.exists():
        print("❌ Inference features not found. Skipping inference.")
        return

    print(f"📦 Model     : {MODEL_FILE.name}")
    print(f"📦 Features  : {INF_FILE.name}")

    # --------------------------------------------------
    # LOAD
    # --------------------------------------------------
    model = joblib.load(MODEL_FILE)
    df = pd.read_parquet(INF_FILE)

    if df.empty:
        print("❌ Inference dataset empty. Skipping.")
        return

    date = df["date"].iloc[0]

    # --------------------------------------------------
    # PREPARE FEATURES
    # --------------------------------------------------
    X = df.drop(columns=["date"], errors="ignore")

    # --------------------------------------------------
    # ALIGN FEATURES WITH TRAINING SCHEMA (CRITICAL)
    # --------------------------------------------------
    if hasattr(model, "feature_names_in_"):
        for col in model.feature_names_in_:
            if col not in X.columns:
                X[col] = 0.0  # missing regime → zero

        # enforce correct order
        X = X[model.feature_names_in_]

    # --------------------------------------------------
    # PREDICT
    # --------------------------------------------------
    prob_up = float(model.predict_proba(X)[0, 1])
    prob_down = 1.0 - prob_up

    # --------------------------------------------------
    # SAVE OUTPUT
    # --------------------------------------------------
    out = pd.DataFrame({
        "date": [date],
        "prob_up": [prob_up],
        "prob_down": [prob_down],
    })

    out.to_parquet(OUT_PQ, index=False)
    out.to_csv(OUT_CSV, index=False)

    print("✅ INFERENCE COMPLETE")
    print(f"📅 Date      : {date.date()}")
    print(f"📈 Prob UP   : {prob_up:.3f}")
    print(f"📉 Prob DOWN : {prob_down:.3f}")
    print(f"💾 Saved     : {OUT_PQ}")


# --------------------------------------------------
# ENTRY
# --------------------------------------------------
if __name__ == "__main__":
    main()
