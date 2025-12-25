#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-13.2 | NIFTY OPTIONS STRIKE SELECTION ENGINE

✔ Direction aware (LONG / SHORT)
✔ Regime aware (trend + vol)
✔ Weekly vs Monthly logic
✔ Backtest + Live safe
"""

from math import floor, ceil

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
NIFTY_STRIKE_GAP = 50  # NSE standard


# --------------------------------------------------
# HELPERS
# --------------------------------------------------
def round_to_strike(price: float, mode: str) -> int:
    """
    mode: ATM / ITM / OTM
    """
    base = round(price / NIFTY_STRIKE_GAP) * NIFTY_STRIKE_GAP

    if mode == "ATM":
        return base
    elif mode == "ITM":
        return base - NIFTY_STRIKE_GAP
    elif mode == "OTM":
        return base + NIFTY_STRIKE_GAP
    else:
        raise ValueError("Invalid strike mode")


# --------------------------------------------------
# CORE STRIKE LOGIC
# --------------------------------------------------
def select_strike(
    spot_price: float,
    signal: str,
    expiry_type: str,
    trend_regime: str,
    vol_regime: str,
):
    """
    Returns:
        option_type: CE / PE
        strike_price: int
        strike_mode: ATM / ITM / OTM
    """

    # -----------------------------
    # OPTION TYPE
    # -----------------------------
    if signal == "LONG":
        option_type = "CE"
    elif signal == "SHORT":
        option_type = "PE"
    else:
        return None, None, None

    # -----------------------------
    # STRIKE MODE LOGIC
    # -----------------------------
    if expiry_type == "WEEKLY":
        if vol_regime == "HIGH_VOL":
            strike_mode = "ATM"
        else:
            strike_mode = "OTM"
    else:  # MONTHLY
        if trend_regime == "BULL":
            strike_mode = "ATM"
        else:
            strike_mode = "ITM"

    # -----------------------------
    # STRIKE CALCULATION
    # -----------------------------
    if option_type == "CE":
        strike = round_to_strike(spot_price, strike_mode)
    else:  # PE
        # reverse ITM/OTM logic for puts
        inverted = {
            "ATM": "ATM",
            "ITM": "OTM",
            "OTM": "ITM",
        }
        strike = round_to_strike(spot_price, inverted[strike_mode])

    return option_type, strike, strike_mode


# --------------------------------------------------
# SELF TEST
# --------------------------------------------------
if __name__ == "__main__":
    tests = [
        # spot, signal, expiry, trend, vol
        (26142, "LONG", "WEEKLY", "BULL", "LOW_VOL"),
        (26142, "LONG", "MONTHLY", "BULL", "LOW_VOL"),
        (26142, "SHORT", "WEEKLY", "BEAR", "HIGH_VOL"),
        (26142, "SHORT", "MONTHLY", "BEAR", "LOW_VOL"),
    ]

    for t in tests:
        opt, strike, mode = select_strike(*t)
        print(f"{t} → {opt} {strike} ({mode})")
