import numpy as np
from pipelines.ml.ensemble_weights import dynamic_weights

# -----------------------------
# Example model probabilities
# -----------------------------
p_xgb = 0.63
p_lgb = 0.58
p_lstm = 0.55

probs = np.array([p_xgb, p_lgb, p_lstm])

# -----------------------------
# Rolling performance (last 20 days)
# -----------------------------
scores = [0.61, 0.57, 0.54]

# -----------------------------
# Regime prior (TREND)
# -----------------------------
regime_weights = [0.45, 0.25, 0.30]

# -----------------------------
# Compute final weights
# -----------------------------
weights = dynamic_weights(
    scores=scores,
    regime_weights=regime_weights,
    alpha=0.6
)

# -----------------------------
# Final ensemble probability
# -----------------------------
P_raw = np.dot(weights, probs)

print("Final Weights :", weights)
print("Ensemble Prob :", round(P_raw, 4))
