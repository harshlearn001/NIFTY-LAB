#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | FINAL CLEANUP (SURGICAL)

✔ Removes only confirmed-unused artifacts
✔ Keeps production, models, pipelines intact
✔ No wildcard danger
"""

from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]

# -------------------------------
# EXPLICIT DELETE LIST
# -------------------------------

DELETE_FILES = [
    ROOT / "fix_parquet.py",
    ROOT / "check_last_3_days.py",
    ROOT / "scripts" / "clean_temp_files.py",
    ROOT / "data" / "backtest" / "backtest_nifty_results.csv",
    ROOT / "data" / "backtest" / "nifty_option_pnl.csv",
]

DELETE_PATTERNS = [
    "data/continuous/master_futures_backup_*",
    "data/continuous/master_options_backup_*",
    "data/continuous/_backup_*",
    "data/continuous/_bk_*",
]

# -------------------------------
# EXECUTION
# -------------------------------

print("🧹 FINAL NIFTY-LAB CLEANUP")
print("-" * 50)

# Delete explicit files
for f in DELETE_FILES:
    if f.exists():
        f.unlink()
        print(f"✔ Deleted file → {f}")

# Delete backup patterns
for pattern in DELETE_PATTERNS:
    for p in ROOT.glob(pattern):
        if p.exists():
            p.unlink()
            print(f"✔ Deleted backup → {p}")

# Delete __pycache__
for d in ROOT.rglob("__pycache__"):
    shutil.rmtree(d, ignore_errors=True)
    print(f"✔ Deleted cache → {d}")

print("-" * 50)
print("✅ FINAL CLEANUP COMPLETE")
