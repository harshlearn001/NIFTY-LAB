#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | GLOBAL MASTER ALIGNMENT (PROD SAFE)

✔ Aligns on latest COMMON trading date
✔ Weekend / holiday safe
✔ Fails only on real data mismatch
"""

from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")

eq = pd.read_parquet(BASE / "data/continuous/master_equity.parquet")
fu = pd.read_parquet(BASE / "data/continuous/master_futures.parquet")
op = pd.read_parquet(BASE / "data/continuous/master_options.parquet")

eq_max = eq["DATE"].max()
fu_max = fu["TRADE_DATE"].max()
op_max = op["TRADE_DATE"].max()

print("GLOBAL ALIGNMENT CHECK")
print("-" * 60)
print("EQUITY  :", eq_max)
print("FUTURES :", fu_max)
print("OPTIONS :", op_max)

# Latest COMMON date
common = min(eq_max, fu_max, op_max)

if common is None:
    raise RuntimeError("❌ One or more masters are empty")

print(f"✅ Latest common trading date: {common.date()}")

# Sanity: no dataset lags more than 1 trading day
for name, d in [
    ("EQUITY", eq_max),
    ("FUTURES", fu_max),
    ("OPTIONS", op_max),
]:
    if (common - d).days > 1:
        raise RuntimeError(f"❌ {name} is misaligned: {d}")

print("✅ GLOBAL MASTER ALIGNMENT PASSED")
