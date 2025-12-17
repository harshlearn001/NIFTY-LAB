#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ML FEATURE BUILDER (TRAINING)

✔ No volume filter (Yahoo index limitation)
✔ Safe date alignment
✔ Target included
✔ GPU-ready
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | HISTORICAL ML FEATURE BUILDER (GPU READY)
====================================================

Builds FULL historical dataset for model training
(no date guards, no leakage, EOD only)
"""

# =================================================
# IMPORTS
# =================================================
from pathlib import Path
import pandas as pd
import numpy as np

# =================================================
# PROJECT ROOT
# =================================================
BASE = Path(r"H:\NIFTY-LAB")

# =================================================
# PATHS (EXPLICIT — NO configs dependency)
# =================================================
CONT_DIR = BASE / "data" / "continuous"
PROC_DIR = BASE / "data" / "processed"

EQ_FILE  = CONT_DIR / "master_equity.parquet"
FUT_FILE = PROC_DIR / "futures_ml" / "nifty_fut_oi_daily.parquet"
PCR_FILE = PROC_DIR / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR  = PROC_DIR / "ml"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PQ  = OUT_DIR / "nifty_ml_features_historical.parquet"
OUT_CSV = OUT_DIR / "nifty_ml_features_historical.csv"


print("📥 Loading historical datasets...")

eq  = pd.read_parquet(EQ_FILE)
fut = pd.read_parquet(FUT_FILE)
pcr = pd.read_parquet(PCR_FILE)

# --------------------------------------------------
# STANDARDISE DATES
# --------------------------------------------------
eq = eq.rename(columns={
    "DATE": "date",
    "CLOSE": "close",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
})
eq["date"] = pd.to_datetime(eq["date"])

fut = fut.rename(columns={
    "TRADE_DATE": "date",
    "price_pct_change": "price_pct",
    "oi_pct_change": "oi_pct",
    "oi_signal": "regime",
})
fut["date"] = pd.to_datetime(fut["date"])

pcr["date"] = pd.to_datetime(pcr["date"])

# --------------------------------------------------
# EQUITY FEATURES
# --------------------------------------------------
eq = eq.sort_values("date")

eq["ret_1d"] = eq["close"].pct_change()
eq["ret_3d"] = eq["close"].pct_change(3)

eq["prev_close"] = eq["close"].shift(1)
eq["tr"] = (eq["close"] - eq["prev_close"]).abs()
eq["atr"] = eq["tr"].rolling(14).mean()
eq["atr_pct"] = eq["atr"] / eq["close"]

eq["range_pct"] = (eq["high"] - eq["low"]) / eq["close"]
eq["dma50"] = eq["close"].rolling(50).mean()
eq["trend_up"] = (eq["close"] > eq["dma50"]).astype(int)

eq_feat = eq[[
    "date", "close", "ret_1d", "ret_3d",
    "atr_pct", "range_pct", "trend_up"
]]

# --------------------------------------------------
# FUTURES FEATURES
# --------------------------------------------------
fut_feat = fut[["date", "price_pct", "oi_pct", "regime"]].copy()
regime_dum = pd.get_dummies(fut_feat["regime"], prefix="regime")
fut_feat = pd.concat(
    [fut_feat.drop(columns="regime"), regime_dum],
    axis=1
)

# --------------------------------------------------
# PCR FEATURES
# --------------------------------------------------
pcr_feat = pcr[["date", "pcr"]].copy()
pcr_feat["pcr_change"] = pcr_feat["pcr"].pct_change()

# --------------------------------------------------
# MERGE (STRICT BUT CORRECT)
# --------------------------------------------------
print("🔗 Aligning Equity + Futures + Options...")

df = eq_feat.merge(fut_feat, on="date", how="inner")
df = df.merge(pcr_feat, on="date", how="inner")

# --------------------------------------------------
# TARGET
# --------------------------------------------------
df["next_close"] = df["close"].shift(-1)
df["next_ret"] = (df["next_close"] - df["close"]) / df["close"]
df["target"] = (df["next_ret"] > 0).astype(int)

# --------------------------------------------------
# FINAL CLEAN
# --------------------------------------------------
df = df.dropna().reset_index(drop=True)

# --------------------------------------------------
# SAVE
# --------------------------------------------------
df.to_parquet(OUT_PQ, index=False)
df.to_csv(OUT_CSV, index=False)

print("\n✅ HISTORICAL ML DATASET READY")
print(f"📦 Parquet : {OUT_PQ}")
print(f"📦 CSV     : {OUT_CSV}")
print(f"📊 Rows    : {len(df)}")
print(f"📊 Columns : {len(df.columns)}")
print("\nSample:")
print(df.head(3))
