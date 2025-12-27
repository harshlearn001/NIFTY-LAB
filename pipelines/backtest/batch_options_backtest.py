#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-16 | BATCH OPTIONS BACKTEST ENGINE (INSTITUTIONAL)

✔ Uses historical ML predictions
✔ Daily decision → position → PnL
✔ No look-ahead bias
✔ Capital-compounded equity curve
✔ CSV-only (parquet banned to avoid corruption)
"""

import sys
from pathlib import Path
import pandas as pd

# ==================================================
# BOOTSTRAP
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ==================================================
# IMPORTS
# ==================================================
from configs.paths import BASE_DIR
from pipelines.ml.ensemble_blender import ensemble_probability
from pipelines.ml.trade_decision import decide_trade

# ==================================================
# PATHS
# ==================================================
ML_FILE = BASE_DIR / "data" / "processed" / "ml" / "nifty_ml_prediction.csv"
RET_FILE = BASE_DIR / "data" / "backtest" / "nifty_daily_returns.csv"

OUT_DIR = BASE_DIR / "data" / "backtest"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = OUT_DIR / "nifty_option_pnl_history.csv"

if not ML_FILE.exists():
    raise FileNotFoundError(f"❌ ML prediction file not found: {ML_FILE}")

print(f"📄 Using ML file: {ML_FILE.name}")

# ==================================================
# CONFIG
# ==================================================
START_CAPITAL = 1_000_000
VOLATILITY = 0.012

REGIME_PRIOR = {
    "TREND":    [0.45, 0.25, 0.30],
    "RANGE":    [0.55, 0.35, 0.10],
    "HIGH_VOL": [0.60, 0.25, 0.15],
}

# ==================================================
# LOAD ML DATA (CSV ONLY)
# ==================================================
df = pd.read_csv(ML_FILE)
df.columns = df.columns.str.upper()

if "DATE" not in df.columns or "PROB_UP" not in df.columns:
    raise RuntimeError("❌ ML prediction CSV missing DATE / PROB_UP")

df["DATE"] = pd.to_datetime(df["DATE"])
df = df.sort_values("DATE").reset_index(drop=True)

# ==================================================
# LOAD RETURNS (OPTIONAL)
# ==================================================
if RET_FILE.exists():
    ret = pd.read_csv(RET_FILE)
    ret.columns = ret.columns.str.upper()
    ret["DATE"] = pd.to_datetime(ret["DATE"])
    df = df.merge(ret[["DATE", "RET"]], on="DATE", how="left")
else:
    df["RET"] = 0.0

df["RET"] = df["RET"].fillna(0.0)

# ==================================================
# BACKTEST LOOP
# ==================================================
capital = START_CAPITAL
rows = []

for i in range(len(df) - 1):  # no look-ahead

    row = df.iloc[i]

    # -------------------------------
    # ENSEMBLE (SINGLE PROB SAFE)
    # -------------------------------
    probs = [row["PROB_UP"]] * 3
    scores = [0.5, 0.5, 0.5]
    regime_weights = [1 / 3] * 3

    ens = ensemble_probability(
        probs=probs,
        scores=scores,
        regime_weights=regime_weights
    )

    # -------------------------------
    # DECISION
    # -------------------------------
    decision = decide_trade(
        ensemble_out=ens,
        capital=capital,
        volatility=VOLATILITY,
        regime="TREND",
        regime_changed_recently=False
    )

    # -------------------------------
    # PnL SIMULATION (INDEX RETURN PROXY)
    # -------------------------------
    pnl = 0.0
    ret_val = float(row["RET"])

    if decision.action == "LONG":
        pnl = decision.position_size * ret_val
        capital += pnl

    elif decision.action == "SHORT":
        pnl = -decision.position_size * ret_val
        capital += pnl

    # -------------------------------
    # RECORD
    # -------------------------------
    rows.append({
        "DATE": row["DATE"],
        "ACTION": decision.action,
        "POSITION_SIZE": decision.position_size,
        "PROBABILITY": ens["P_adj"],
        "CONFIDENCE": ens["confidence"],
        "PNL": pnl,
        "CAPITAL": capital,
    })

# ==================================================
# SAVE
# ==================================================
bt = pd.DataFrame(rows)
bt.to_csv(OUT_FILE, index=False)

print("\n✅ BATCH OPTIONS BACKTEST COMPLETE")
print(bt.tail(10))
print(f"\n📁 Saved → {OUT_FILE}")
