#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | Backtest Decision Pipeline (Schema & Path Safe)
-----------------------------------------------------------
✔ Auto-detects historical prediction file
✔ Supports CSV or Parquet
✔ Uses SAME ensemble + decision logic
✔ No look-ahead bias
"""

# ==========================================================
# PATH FIX
# ==========================================================
import sys
from pathlib import Path
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ==========================================================
# IMPORTS
# ==========================================================
from pipelines.ml.ensemble_blender import ensemble_probability
from pipelines.ml.trade_decision import decide_trade

# ==========================================================
# PATHS
# ==========================================================
BASE = ROOT
ML_DIRS = [
    BASE / "data" / "processed" / "ml",
    Path(r"H:\NIFTY-LAB-Trial\data\processed\ml"),
]

candidates = []
for d in ML_DIRS:
    if d.exists():
        candidates.extend(d.glob("*prediction_historical*.*"))

if not candidates:
    raise FileNotFoundError(
        "❌ No historical ML prediction file found.\n"
        f"Searched:\n" + "\n".join(str(d) for d in ML_DIRS)
    )

# choose latest
PRED_HIST = max(candidates, key=lambda p: p.stat().st_mtime)
print(f"📄 Using historical prediction file: {PRED_HIST}")


if not candidates:
    raise FileNotFoundError(
        f"❌ No historical ML prediction file found in:\n{ML_DIR}"
    )

PRED_HIST = candidates[0]
print(f"📄 Using historical prediction file: {PRED_HIST.name}")

# ==========================================================
# LOAD DATA (CSV or PARQUET)
# ==========================================================
if PRED_HIST.suffix == ".parquet":
    df = pd.read_parquet(PRED_HIST)
else:
    df = pd.read_csv(PRED_HIST)

df.columns = df.columns.str.upper()

if "DATE" not in df.columns:
    raise RuntimeError("❌ DATE column missing in historical predictions")

df = df.sort_values("DATE").reset_index(drop=True)

# ==========================================================
# CONFIG
# ==========================================================
CAPITAL_START = 1_000_000
VOLATILITY = 0.012

REGIME_PRIOR = {
    "TREND":    [0.45, 0.25, 0.30],
    "RANGE":    [0.55, 0.35, 0.10],
    "HIGH_VOL": [0.60, 0.25, 0.15],
}

capital = CAPITAL_START
rows = []

# ==========================================================
# BACKTEST LOOP (DAY BY DAY)
# ==========================================================
for i in range(len(df)):

    row = df.iloc[i]

    # ------------------------
    # Detect prediction schema
    # ------------------------
    if "PROB_UP" in row.index:
        p_up = float(row["PROB_UP"])
        probs = [p_up, p_up, p_up]
        scores = [0.5, 0.5, 0.5]
        regime_weights = [1/3, 1/3, 1/3]
    else:
        probs = [
            float(row["P_XGB"]),
            float(row["P_LGBM"]),
            float(row["P_LSTM"]),
        ]
        scores = [
            float(row.get("SCORE_XGB", 0.5)),
            float(row.get("SCORE_LGBM", 0.5)),
            float(row.get("SCORE_LSTM", 0.5)),
        ]
        regime = str(row.get("REGIME", "TREND")).upper()
        regime_weights = REGIME_PRIOR.get(regime, REGIME_PRIOR["TREND"])

    # ------------------------
    # Ensemble
    # ------------------------
    ensemble_out = ensemble_probability(
        probs=probs,
        scores=scores,
        regime_weights=regime_weights
    )

    # ------------------------
    # Decision
    # ------------------------
    decision = decide_trade(
        ensemble_out=ensemble_out,
        capital=capital,
        volatility=VOLATILITY,
        regime=row.get("REGIME", "TREND"),
        regime_changed_recently=False
    )

    # ------------------------
    # Simulate PnL (NEXT DAY)
    # Requires RET column
    # ------------------------
    pnl = 0.0
    if decision.action == "LONG" and i < len(df) - 1 and "RET" in df.columns:
        ret = float(df.iloc[i + 1]["RET"])
        pnl = decision.position_size * ret
        capital += pnl

    # ------------------------
    # Record
    # ------------------------
    rows.append({
        "DATE": row["DATE"],
        "ACTION": decision.action,
        "POSITION_SIZE": decision.position_size,
        "CONFIDENCE": ensemble_out["confidence"],
        "PROBABILITY": ensemble_out["P_adj"],
        "PNL": pnl,
        "CAPITAL": capital,
    })

# ==========================================================
# SAVE RESULTS
# ==========================================================
bt = pd.DataFrame(rows)
bt.to_csv(OUT_FILE, index=False)

print("\n✅ BACKTEST COMPLETE")
print(bt.tail(10))
print(f"\n📁 Saved to: {OUT_FILE}")
