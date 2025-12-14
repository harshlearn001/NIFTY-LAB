#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY Futures OI Regime Performance Statistics
---------------------------------------------
Answers:
- Which OI regime works?
- What is the win rate?
- What is the average next-day return?
"""

# =================================================
# BOOTSTRAP PROJECT ROOT
# =================================================
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
DATA_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"
OUT_DIR = PROC_DIR / "futures_ml"
OUT_FILE = OUT_DIR / "nifty_fut_oi_regime_stats.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading futures OI daily data...")

if not DATA_FILE.exists():
    raise FileNotFoundError(f"Missing file: {DATA_FILE}")

df = pd.read_parquet(DATA_FILE).copy()

df = df.sort_values("date").reset_index(drop=True)

# =================================================
# NEXT DAY RETURN
# =================================================
df["next_close"] = df["close"].shift(-1)
df["next_ret_pct"] = (df["next_close"] / df["close"] - 1) * 100

# Drop last row (no next day)
df = df.dropna(subset=["next_ret_pct"])

# =================================================
# WIN / LOSS FLAG
# =================================================
df["win"] = (df["next_ret_pct"] > 0).astype(int)

# =================================================
# REGIME STREAK CALCULATION
# =================================================
df["regime_change"] = (df["regime"] != df["regime"].shift()).astype(int)
df["regime_group"] = df["regime_change"].cumsum()

streaks = (
    df.groupby(["regime", "regime_group"])
      .size()
      .reset_index(name="streak_len")
)

avg_streak = (
    streaks.groupby("regime")["streak_len"]
            .mean()
            .rename("avg_streak_len")
)

# =================================================
# REGIME PERFORMANCE STATS
# =================================================
stats = (
    df.groupby("regime")
      .agg(
          days=("regime", "count"),
          win_rate_pct=("win", "mean"),
          avg_next_ret_pct=("next_ret_pct", "mean"),
          median_next_ret_pct=("next_ret_pct", "median"),
          best_day_pct=("next_ret_pct", "max"),
          worst_day_pct=("next_ret_pct", "min"),
      )
      .reset_index()
)

stats["win_rate_pct"] *= 100

# Merge streak stats
stats = stats.merge(avg_streak, on="regime", how="left")

# Sort by average return
stats = stats.sort_values("avg_next_ret_pct", ascending=False)

# =================================================
# SAVE OUTPUT
# =================================================
stats.to_parquet(OUT_FILE, index=False)
stats.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

# =================================================
# PRINT SUMMARY
# =================================================
print("\n📊 OI REGIME PERFORMANCE SUMMARY\n")
print(stats.round(3).to_string(index=False))

print("\n✅ Regime performance stats built successfully")
print(f"📦 Parquet: {OUT_FILE}")
print(f"📄 CSV: {OUT_FILE.with_suffix('.csv')}")
