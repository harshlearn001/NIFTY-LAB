#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-11 | CAPITAL MANAGER (PRODUCTION SAFE)

✔ Drawdown-aware risk control
✔ Regime-adjusted position risk
✔ Capital protection mode
✔ Stateless logic + lightweight state container
✔ Backtest & Live compatible
"""

# ==================================================
# CAPITAL STATE (TRACKS PEAK EQUITY)
# ==================================================
class CapitalState:
    """
    Keeps track of peak equity to compute drawdown.
    """

    def __init__(self, initial_equity: float = 1.0):
        self.peak_equity = initial_equity

    def update(self, equity: float):
        """
        Update peak equity.
        """
        if equity > self.peak_equity:
            self.peak_equity = equity

    def drawdown(self, equity: float) -> float:
        """
        Returns drawdown as a negative number.
        Example: -0.12 = -12%
        """
        return (equity / self.peak_equity) - 1.0


# ==================================================
# RISK COMPUTATION ENGINE
# ==================================================
def compute_position_risk(
    base_risk: float,
    drawdown: float,
    regime_multiplier: float
):
    """
    Compute final risk per trade using:
    - Base risk
    - Drawdown-based scaling
    - Regime-based scaling

    Returns:
        risk_pct (float),
        reason (str)
    """

    # -----------------------------
    # DRAWdown ADAPTATION
    # -----------------------------
    if drawdown > -0.10:
        dd_mult = 1.0
        dd_label = "DD_OK"
    elif drawdown > -0.20:
        dd_mult = 0.5
        dd_label = "DD_WARNING"
    elif drawdown > -0.30:
        dd_mult = 0.25
        dd_label = "DD_DANGER"
    else:
        return 0.0, "CAPITAL_PROTECTION"

    # -----------------------------
    # FINAL RISK
    # -----------------------------
    final_risk = base_risk * dd_mult * regime_multiplier

    reason = f"{dd_label}|REG={regime_multiplier:.2f}"
    return round(final_risk, 6), reason


# ==================================================
# SELF TEST
# ==================================================
if __name__ == "__main__":
    print("📊 PHASE-11 | CAPITAL MANAGER SELF TEST")

    capital = CapitalState(initial_equity=1.0)

    test_cases = [
        (1.00, 1.0),
        (0.92, 1.0),
        (0.80, 0.5),
        (0.65, 0.5),
        (0.45, 0.3),
    ]

    BASE_RISK = 0.01

    for equity, regime_mult in test_cases:
        capital.update(equity)
        dd = capital.drawdown(equity)

        risk, reason = compute_position_risk(
            base_risk=BASE_RISK,
            drawdown=dd,
            regime_multiplier=regime_mult
        )

        print(
            f"Equity={equity:.2f} | "
            f"DD={dd:.2%} | "
            f"Risk={risk:.4f} | "
            f"Reason={reason}"
        )
