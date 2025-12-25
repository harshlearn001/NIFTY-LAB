#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-6 | REGIME RISK POLICY
Defines base risk rules per market regime
"""

def get_regime_policy(trend_regime: str, vol_regime: str):
    policy = {
        ("BULL", "LOW_VOL"): {
            "max_position": 1.0,
            "risk_mult": 1.0,
            "allow_long": True,
            "allow_short": False,
            "notes": "Aggressive longs only",
        },
        ("BULL", "MID_VOL"): {
            "max_position": 0.7,
            "risk_mult": 0.8,
            "allow_long": True,
            "allow_short": False,
            "notes": "Cautious longs",
        },
        ("BULL", "HIGH_VOL"): {
            "max_position": 0.5,
            "risk_mult": 0.6,
            "allow_long": True,
            "allow_short": False,
            "notes": "Capital protection",
        },
        ("BEAR", "LOW_VOL"): {
            "max_position": 0.6,
            "risk_mult": 0.8,
            "allow_long": False,
            "allow_short": True,
            "notes": "Controlled shorts",
        },
        ("BEAR", "MID_VOL"): {
            "max_position": 0.4,
            "risk_mult": 0.6,
            "allow_long": False,
            "allow_short": True,
            "notes": "Defensive shorts",
        },
        ("BEAR", "HIGH_VOL"): {
            "max_position": 0.2,
            "risk_mult": 0.4,
            "allow_long": False,
            "allow_short": False,
            "notes": "No trading",
        },
        ("SIDEWAYS", "LOW_VOL"): {
            "max_position": 0.3,
            "risk_mult": 0.4,
            "allow_long": False,
            "allow_short": False,
            "notes": "Flat regime",
        },
    }

    return policy.get(
        (trend_regime, vol_regime),
        {"max_position": 0.0, "risk_mult": 0.0, "allow_long": False, "allow_short": False}
    )


if __name__ == "__main__":
    for k in [
        ("BULL", "LOW_VOL"),
        ("BULL", "HIGH_VOL"),
        ("BEAR", "LOW_VOL"),
        ("SIDEWAYS", "LOW_VOL"),
    ]:
        print(k, "→", get_regime_policy(*k))
