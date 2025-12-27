#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Ensemble Blender
----------------------------------
✔ Dynamic regime + performance weights
✔ Disagreement penalty
✔ Confidence score
✔ Safe to import OR run directly
"""

# ==========================================================
# PATH FIX (allows running from anywhere)
# ==========================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # H:\NIFTY-LAB
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ==========================================================
# IMPORTS
# ==========================================================
import numpy as np
from pipelines.ml.ensemble_weights import dynamic_weights

# ==========================================================
# CORE ENSEMBLE FUNCTION
# ==========================================================
def ensemble_probability(
    probs,                # list/array of model probabilities
    scores,               # rolling model performance scores
    regime_weights,       # regime prior weights (sum = 1)
    alpha=0.6,
    beta=5.0,
    K=2.5
):
    """
    Returns ensemble probability, confidence & diagnostics
    """

    probs = np.asarray(probs, dtype=float)

    # -----------------------------
    # 1️⃣ Dynamic weights
    # -----------------------------
    weights = dynamic_weights(
        scores=scores,
        regime_weights=regime_weights,
        alpha=alpha,
        beta=beta
    )

    # -----------------------------
    # 2️⃣ Weighted probability
    # -----------------------------
    P_raw = float(np.dot(weights, probs))

    # -----------------------------
    # 3️⃣ Disagreement (weighted std)
    # -----------------------------
    sigma = float(np.sqrt(np.sum(weights * (probs - P_raw) ** 2)))

    # -----------------------------
    # 4️⃣ Agreement score
    # -----------------------------
    agreement = 1.0 - min(1.0, K * sigma)

    # -----------------------------
    # 5️⃣ Confidence-adjusted prob
    # -----------------------------
    P_adj = 0.5 + agreement * (P_raw - 0.5)

    # -----------------------------
    # 6️⃣ Confidence score (0–1)
    # -----------------------------
    confidence = agreement * abs(P_adj - 0.5) * 2.0

    return {
        "P_raw": round(P_raw, 6),
        "P_adj": round(P_adj, 6),
        "confidence": round(confidence, 6),
        "agreement": round(agreement, 6),
        "sigma": round(sigma, 6),
        "weights": np.round(weights, 6),
    }

# ==========================================================
# SAFE TEST BLOCK (ONLY RUNS WHEN EXECUTED DIRECTLY)
# ==========================================================
if __name__ == "__main__":

    print("\n🔍 Running Ensemble Blender Self-Test\n")

    # Example model probabilities
    probs = [0.63, 0.58, 0.55]      # XGB, LGBM, LSTM

    # Rolling performance (last ~20 days)
    scores = [0.62, 0.58, 0.55]

    # Regime prior (TREND example)
    regime_weights = [0.45, 0.25, 0.30]

    out = ensemble_probability(
        probs=probs,
        scores=scores,
        regime_weights=regime_weights
    )

    for k, v in out.items():
        print(f"{k:12s} : {v}")

    print("\n✅ Ensemble blender test completed successfully\n")
