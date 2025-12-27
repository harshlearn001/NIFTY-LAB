# -*- coding: utf-8 -*-

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics import log_loss
from scipy.optimize import minimize


class TemperatureScaler(BaseEstimator, TransformerMixin):
    """
    Probability calibration using temperature scaling
    Pickle-safe (must live in its own module)
    """

    def __init__(self):
        self.temperature_ = 1.0

    def fit(self, logits, y):
        logits = np.asarray(logits).reshape(-1, 1)
        y = np.asarray(y)

        def loss_fn(t):
            t = max(t[0], 1e-6)
            probs = 1 / (1 + np.exp(-logits / t))
            return log_loss(y, probs)

        res = minimize(loss_fn, x0=[1.0], bounds=[(0.05, 10.0)])
        self.temperature_ = res.x[0]
        return self

    def transform(self, logits):
        logits = np.asarray(logits).reshape(-1, 1)
        return 1 / (1 + np.exp(-logits / self.temperature_))
