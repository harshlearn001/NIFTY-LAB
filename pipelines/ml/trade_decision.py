#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Trade Decision Engine
----------------------------------
✔ Converts ensemble output → action
✔ Risk-aware, regime-safe
✔ Deterministic sizing
"""

from dataclasses import dataclass
from typing import Dict

# -------------------------------
# CONFIG (tune once, rarely)
# -------------------------------
LONG_TH = 0.60
SHORT_TH = 0.40
MIN_CONF = 0.25
MIN_AGREEMENT = 0.45

BASE_RISK_PER_TRADE = 0.01   # 1% capital
MAX_POSITION_CAP = 0.03      # cap at 3% capital


@dataclass
class Decision:
    action: str
    position_size: float
    reason: str
    diagnostics: Dict


def decide_trade(
    ensemble_out: Dict,
    capital: float,
    volatility: float,      # e.g., ATR% or daily vol (decimal)
    regime: str,            # "TREND", "RANGE", "HIGH_VOL"
    regime_changed_recently: bool = False
) -> Decision:
    """
    Decide trade action and position size.
    """

    P = ensemble_out["P_adj"]
    C = ensemble_out["confidence"]
    A = ensemble_out["agreement"]

    # ---------------------------
    # KILL SWITCHES
    # ---------------------------
    if A < MIN_AGREEMENT:
        return Decision(
            action="HOLD",
            position_size=0.0,
            reason="Low model agreement",
            diagnostics=ensemble_out
        )

    if regime_changed_recently:
        return Decision(
            action="HOLD",
            position_size=0.0,
            reason="Recent regime change",
            diagnostics=ensemble_out
        )

    if C < MIN_CONF:
        return Decision(
            action="HOLD",
            position_size=0.0,
            reason="Low confidence",
            diagnostics=ensemble_out
        )

    # ---------------------------
    # DIRECTION
    # ---------------------------
    if P > LONG_TH:
        action = "LONG"
    elif P < SHORT_TH:
        action = "SHORT"
    else:
        return Decision(
            action="HOLD",
            position_size=0.0,
            reason="Probability in no-trade zone",
            diagnostics=ensemble_out
        )

    # ---------------------------
    # POSITION SIZING
    # ---------------------------
    # Risk scales with confidence, inversely with volatility
    raw_size = capital * BASE_RISK_PER_TRADE * C / max(volatility, 1e-6)

    # Cap exposure
    max_size = capital * MAX_POSITION_CAP
    position_size = min(raw_size, max_size)

    # Regime adjustment
    if regime == "HIGH_VOL":
        position_size *= 0.6
    elif regime == "RANGE":
        position_size *= 0.8

    return Decision(
        action=action,
        position_size=round(position_size, 2),
        reason="All conditions satisfied",
        diagnostics=ensemble_out
    )


# -------------------------------
# SELF TEST
# -------------------------------
if __name__ == "__main__":

    sample_ensemble = {
        "P_adj": 0.585,
        "confidence": 0.155,
        "agreement": 0.91,
        "sigma": 0.03,
        "weights": [0.43, 0.28, 0.29],
        "P_raw": 0.593
    }

    out = decide_trade(
        ensemble_out=sample_ensemble,
        capital=1_000_000,     # ₹10L
        volatility=0.012,      # 1.2%
        regime="TREND",
        regime_changed_recently=False
    )

    print(out)
