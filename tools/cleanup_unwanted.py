#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
NIFTY-LAB | SAFE CLEANUP SCRIPT

✔ Deletes only temp / cache / obsolete artifacts
✔ Does NOT touch core pipelines, models, or CSV outputs
✔ Dry-run mode supported
"""

from pathlib import Path
import shutil

# ===============================
# CONFIG
# ===============================
ROOT = Path(__file__).resolve().parents[1]
DRY_RUN = False   # 🔴 SET True FIRST, then False when satisfied

# ===============================
# DELETE RULES
# ===============================

DELETE_DIR_NAMES = {
    "__pycache__",
}

DELETE_FILE_EXT = {
    ".pyc",
    ".log",
}

DELETE_FILE_PATTERNS = [
    "*_prediction_historical.parquet",
    "*_old.py",
    "*_bak.py",
]

DELETE_DIR_PATTERNS = [
    "analytics/*/output",
]

# ===============================
# HELPERS
# ===============================

def delete_path(p: Path):
    if DRY_RUN:
        print(f"[DRY] Would delete → {p}")
        return

    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)
        print(f"✔ Deleted → {p}")
    except Exception as e:
        print(f"⚠ Failed → {p} | {e}")

# ===============================
# EXECUTION
# ===============================

print("🧹 NIFTY-LAB CLEANUP STARTED")
print(f"📂 Root: {ROOT}")
print(f"🧪 Dry run: {DRY_RUN}")
print("-" * 50)

# --- Delete directories by name ---
for d in ROOT.rglob("*"):
    if d.is_dir() and d.name in DELETE_DIR_NAMES:
        delete_path(d)

# --- Delete files by extension ---
for f in ROOT.rglob("*"):
    if f.is_file() and f.suffix in DELETE_FILE_EXT:
        delete_path(f)

# --- Delete files by pattern ---
for pattern in DELETE_FILE_PATTERNS:
    for f in ROOT.rglob(pattern):
        delete_path(f)

# --- Delete dirs by pattern ---
for pattern in DELETE_DIR_PATTERNS:
    for d in ROOT.glob(pattern):
        if d.exists():
            delete_path(d)

print("-" * 50)
print("✅ CLEANUP COMPLETE")
