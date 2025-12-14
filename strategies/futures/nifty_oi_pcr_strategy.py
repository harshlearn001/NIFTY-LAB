#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY Futures OI + Options PCR Strategy
--------------------------------------
Final alpha strategy combining:
- Futures OI regimes
- Options Put–Call Ratio (PCR)

Output:
- High-confidence BUY / SELL / NO_TRADE signals
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
OI_SIGNAL_FILE = PROC_DIR / "futures_ml" / "nifty_oi_trade_signals.parquet"
PCR_FILE = PROC_DIR / "options_ml" / "nifty_pcr_daily.parquet"

OUT_DIR = PROC_DIR / "futures_ml"
OUT_FILE = OUT_DIR / "nifty_oi_pcr_trade_signals.parquet"

# =================================================
# LOAD DATA
# =================================================
print("📥 Loading futures OI signals and PCR data...")

df_oi = pd.read_parquet(OI_SIGNAL_FILE)
df_pcr = pd.read_parquet(PCR_FILE)

df_oi["date"] = pd.to_datetime(df_oi["date"])
df_pcr["date"] = pd.to_datetime(df_pcr["date"])

# =================================================
# MERGE (LEFT JOIN)
# =================================================
df = df_oi.merge(
    df_pcr[["date", "pcr"]],
    on="date",
    how="left"
)

# =================================================
# FINAL SIGNAL LOGIC
# =================================================
def final_signal(row):
    if row["signal"] == "BUY" and row["pcr"] >= 1.0:
        return "BUY"
    if row["signal"] == "SELL" and row["pcr"] <= 0.9:
        return "SELL"
    return "NO_TRADE"

df["final_signal"] = df.apply(final_signal, axis=1)
df["confirmation"] = df["final_signal"].apply(
    lambda x: "YES" if x != "NO_TRADE" else "NO"
)

# =================================================
# FINAL OUTPUT
# =================================================
out_cols = [
    "date",
    "expiry",
    "close",
    "regime",
    "signal",
    "pcr",
    "final_signal",
    "confirmation",
    "confidence",
]

df_out = df[out_cols].sort_values("date")

df_out.to_parquet(OUT_FILE, index=False)
df_out.to_csv(OUT_FILE.with_suffix(".csv"), index=False)

# =================================================
# SUMMARY
# =================================================
print("\n📊 FINAL SIGNAL DISTRIBUTION")
print(df_out["final_signal"].value_counts())

print("\n🎯 SAMPLE OUTPUT")
print(df_out.tail(5))

print("\n✅ NIFTY OI + PCR strategy built successfully")
print(f"📦 Parquet: {OUT_FILE}")
print(f"📄 CSV: {OUT_FILE.with_suffix('.csv')}")
