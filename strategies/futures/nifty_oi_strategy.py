#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY Futures OI-Based Trading Strategy
--------------------------------------
Uses validated OI regimes + regime performance stats
to generate BUY / SELL / NO_TRADE signals with confidence scores.
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # H:\NIFTY-LAB
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# =================================================
# IMPORTS
# =================================================
import pandas as pd
from configs.paths import PROC_DIR

# =================================================
# PATHS
# =================================================
OI_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"
STATS_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_regime_stats.parquet"

OUT_DIR = PROC_DIR / "futures_ml"
OUT_FILE = OUT_DIR / "nifty_oi_trade_signals.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading OI regimes and stats...")

df = pd.read_parquet(OI_FILE).sort_values("date")
stats = pd.read_parquet(STATS_FILE)

# =================================================
# BUILD CONFIDENCE MAP
# =================================================
# Normalize confidence using win rate + avg return
stats["confidence"] = (
    0.7 * stats["win_rate_pct"] +
    0.3 * (stats["avg_next_ret_pct"] * 100)
)

confidence_map = (
    stats.set_index("regime")["confidence"]
         .round(2)
         .to_dict()
)

# =================================================
# SIGNAL LOGIC
# =================================================
def generate_signal(regime):
    if regime in ("SHORT_COVERING", "LONG_BUILDUP"):
        return "BUY"
    if regime == "SHORT_BUILDUP":
        return "SELL"
    return "NO_TRADE"

df["signal"] = df["regime"].apply(generate_signal)
df["confidence"] = df["regime"].map(confidence_map).fillna(0)

# =================================================
# FINAL OUTPUT
# =================================================
out_cols = [
    "date",
    "expiry",
    "close",
    "regime",
    "signal",
    "confidence",
]

df_out = df[out_cols].copy()

# Save
df_out.to_parquet(OUT_FILE, index=False)
df_out.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

# =================================================
# SUMMARY
# =================================================
print("\n📊 SIGNAL DISTRIBUTION")
print(df_out["signal"].value_counts())

print("\n🎯 SAMPLE OUTPUT")
print(df_out.tail(5))

print("\n✅ NIFTY OI strategy signals generated")
print(f"📦 Parquet: {OUT_FILE}")
print(f"📄 CSV: {OUT_FILE.with_suffix('.csv')}")
