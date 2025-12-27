# ==================================================
# NIFTY-LAB | Dynamic Ensemble Weighting
# ==================================================

import numpy as np


def softmax(scores, beta=5.0):
    scores = np.asarray(scores, dtype=float)
    z = scores * beta
    e = np.exp(z - np.max(z))   # stability
    return e / e.sum()


def dynamic_weights(scores, regime_weights, alpha=0.6, beta=5.0):
    scores = np.asarray(scores, dtype=float)
    regime_weights = np.asarray(regime_weights, dtype=float)

    assert len(scores) == len(regime_weights), "Length mismatch"
    assert np.isclose(regime_weights.sum(), 1.0), "Regime weights must sum to 1"

    learned = softmax(scores, beta=beta)
    final = alpha * regime_weights + (1 - alpha) * learned
    return final / final.sum()
