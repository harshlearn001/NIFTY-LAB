#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | WALK-FORWARD BACKTEST (PHASE-6 FINAL)

✔ ML confidence filtering
✔ ATR-based SL + Target
✔ Regime Kill Switch (PHASE-7)
✔ Capital Manager (PHASE-11)
✔ Drawdown adaptive risk
✔ Capital protection mode
✔ As-of joins (NO lookahead)
✔ Production safe
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# --------------------------------------------------
# BOOTSTRAP
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR, CONT_DIR
from strategies.final.nifty_ml_oi_pcr_final_strategy import generate_signal
from strategies.risk.regime_kill_switch import regime_kill_switch
from strategies.risk.capital_manager import CapitalState, compute_position_risk

print("🧠 ML MODE → BACKTEST (PHASE-6)")
print("🚀 NIFTY FINAL STRATEGY (ML + OI + PCR + REGIME + CAPITAL ENGINE)")

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
BASE_RISK     = 0.01      # Max 1% risk
CONF_THRESH   = 0.60
ATR_SL_MULT   = 1.5
ATR_TGT_MULT  = 3.0
MAX_HOLD_DAYS = 10

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------
ml     = pd.read_parquet(BASE_DIR / "data/processed/ml/nifty_ml_prediction_historical.parquet")
oi     = pd.read_parquet(BASE_DIR / "data/processed/futures_ml/nifty_fut_oi_daily.parquet")
pcr    = pd.read_parquet(BASE_DIR / "data/processed/options_ml/nifty_pcr_daily.parquet")
regime = pd.read_parquet(CONT_DIR / "nifty_regime.parquet")
price  = pd.read_parquet(CONT_DIR / "nifty_continuous.parquet")

# --------------------------------------------------
# DATE NORMALIZATION
# --------------------------------------------------
def norm(df):
    for c in ["date", "DATE", "TRADE_DATE"]:
        if c in df.columns:
            df["date"] = pd.to_datetime(df[c]).dt.date
            return df
    raise RuntimeError("No date column found")

ml, oi, pcr, regime, price = map(norm, [ml, oi, pcr, regime, price])

# --------------------------------------------------
# ATR CALCULATION
# --------------------------------------------------
px = price.sort_values("date").copy()

tr = np.maximum(
    px["HIGH"] - px["LOW"],
    np.maximum(
        (px["HIGH"] - px["CLOSE"].shift()).abs(),
        (px["LOW"] - px["CLOSE"].shift()).abs()
    )
)
px["ATR"] = tr.rolling(14).mean()

# --------------------------------------------------
# OI REGIME COLUMN
# --------------------------------------------------
oi_reg_col = next(c for c in ["regime", "oi_regime", "oi_signal"] if c in oi.columns)

# --------------------------------------------------
# CAPITAL STATE
# --------------------------------------------------
capital = CapitalState(initial_equity=1.0)

equity = 1.0
peak = 1.0
records = []

dates = sorted(ml["date"].unique())

# --------------------------------------------------
# BACKTEST LOOP
# --------------------------------------------------
for d in dates[:-1]:

    row_ml = ml[ml["date"] == d].iloc[-1]

    # ---------- ML CONFIDENCE FILTER ----------
    confidence = max(row_ml["prob_up"], row_ml["prob_down"])
    if confidence < CONF_THRESH:
        continue

    row_oi  = oi[oi["date"] <= d].sort_values("date").tail(1)
    row_pcr = pcr[pcr["date"] <= d].sort_values("date").tail(1)
    row_reg = regime[regime["date"] <= d].sort_values("date").tail(1)

    if row_oi.empty or row_reg.empty:
        continue

    pcr_val = float(row_pcr["pcr"].values[0]) if not row_pcr.empty else 1.0

    signal, _, reason = generate_signal(
        row_ml["prob_up"],
        row_ml["prob_down"],
        row_oi.iloc[0][oi_reg_col],
        pcr_val,
        row_reg.iloc[0]["TREND_REGIME"],
        row_reg.iloc[0]["VOL_REGIME"],
    )

    if signal == "NO_TRADE":
        continue

    # ---------- REGIME KILL SWITCH ----------
    allowed, size_mult, verdict = regime_kill_switch(
        row_reg.iloc[0]["TREND_REGIME"],
        row_reg.iloc[0]["VOL_REGIME"],
    )

    if not allowed:
        continue

    today  = px[px["date"] == d]
    future = px[px["date"] > d].head(MAX_HOLD_DAYS)

    if today.empty or future.empty or pd.isna(today["ATR"].values[0]):
        continue

    entry = today["CLOSE"].values[0]
    atr   = today["ATR"].values[0]

    sl_dist  = ATR_SL_MULT * atr
    tgt_dist = ATR_TGT_MULT * atr

    # ---------- CAPITAL ENGINE ----------
    capital.update(equity)
    drawdown = capital.drawdown(equity)

    risk_pct, risk_reason = compute_position_risk(
        base_risk=BASE_RISK,
        drawdown=drawdown,
        regime_multiplier=size_mult,
    )

    if risk_pct == 0.0:
        continue

    risk_amt = equity * risk_pct
    qty = risk_amt / sl_dist

    exit_price = future.iloc[-1]["CLOSE"]
    outcome = "TIME_EXIT"

    for _, r in future.iterrows():
        if signal == "LONG":
            if r["LOW"] <= entry - sl_dist:
                exit_price = entry - sl_dist
                outcome = "SL"
                break
            if r["HIGH"] >= entry + tgt_dist:
                exit_price = entry + tgt_dist
                outcome = "TARGET"
                break
        else:
            if r["HIGH"] >= entry + sl_dist:
                exit_price = entry + sl_dist
                outcome = "SL"
                break
            if r["LOW"] <= entry - tgt_dist:
                exit_price = entry - tgt_dist
                outcome = "TARGET"
                break

    pnl = (exit_price - entry) * qty
    if signal == "SHORT":
        pnl = -pnl

    equity += pnl
    peak = max(peak, equity)
    dd = (equity / peak) - 1

    records.append({
        "date": d,
        "signal": signal,
        "confidence": round(confidence, 3),
        "risk_pct": round(risk_pct, 5),
        "risk_reason": risk_reason,
        "regime_verdict": verdict,
        "qty": round(qty, 2),
        "entry": round(entry, 2),
        "exit": round(exit_price, 2),
        "pnl": round(pnl, 5),
        "equity": round(equity, 4),
        "drawdown": round(dd, 4),
        "exit_reason": outcome,
    })

# --------------------------------------------------
# RESULTS
# --------------------------------------------------
bt = pd.DataFrame(records)

print("\n📊 BACKTEST SUMMARY")
print("----------------------------------------")

if bt.empty:
    print("No trades generated.")
else:
    print(f"Trades       : {len(bt)}")
    print(f"Win rate     : {(bt['pnl'] > 0).mean():.2%}")
    print(f"Final equity : {equity:.2f}")
    print(f"Max DD       : {bt['drawdown'].min():.2%}")
    print(f"Avg pnl/trade: {bt['pnl'].mean():.4f}")

OUT = BASE_DIR / "data/backtest_nifty_results.csv"
bt.to_csv(OUT, index=False)
print(f"\n💾 Saved → {OUT}")
