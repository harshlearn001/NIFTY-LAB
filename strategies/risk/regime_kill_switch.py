#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-7.3 | REGIME KILL SWITCH
Hard block / reduce trading based on regime performance
"""

def regime_kill_switch(trend: str, vol: str):
    """
    Returns:
        allowed (bool),
        size_multiplier (float),
        verdict (str)
    """

    if trend == "BULL" and vol == "LOW_VOL":
        return True, 0.5, "REDUCE"

    if trend == "BULL" and vol == "MID_VOL":
        return True, 0.5, "REDUCE"

    if trend == "BULL" and vol == "HIGH_VOL":
        return True, 0.3, "REDUCE"

    if trend == "BEAR":
        return False, 0.0, "BLOCK"

    return False, 0.0, "BLOCK"


if __name__ == "__main__":
    tests = [
        ("BULL", "LOW_VOL"),
        ("BULL", "MID_VOL"),
        ("BEAR", "LOW_VOL"),
        ("BEAR", "HIGH_VOL"),
    ]

    for t, v in tests:
        print(f"{t} | {v} →", regime_kill_switch(t, v))
