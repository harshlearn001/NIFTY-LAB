#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
PHASE-15.1 | EQUITY CURVE & DRAWDOWN PLOTTER

✔ Uses equity curve CSV
✔ Plots equity & drawdown
✔ Investor-grade visualization
"""

import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ==================================================
# BOOTSTRAP
# ==================================================
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from configs.paths import BASE_DIR

# ==================================================
# PATHS
# ==================================================
FILE = BASE_DIR / "data" / "analysis" / "nifty_equity_curve.csv"

if not FILE.exists():
    raise FileNotFoundError("❌ Run equity_curve_analyzer.py first")

df = pd.read_csv(FILE)
df["DATE"] = pd.to_datetime(df["DATE"])

# ==================================================
# PLOT
# ==================================================
plt.figure()
plt.plot(df["DATE"], df["EQUITY"])
plt.title("NIFTY Options Strategy – Equity Curve")
plt.xlabel("Date")
plt.ylabel("Equity (₹)")
plt.grid(True)
plt.tight_layout()
plt.show()

plt.figure()
plt.plot(df["DATE"], df["DRAWDOWN"])
plt.title("NIFTY Options Strategy – Drawdown")
plt.xlabel("Date")
plt.ylabel("Drawdown (₹)")
plt.grid(True)
plt.tight_layout()
plt.show()
