#!/usr/bin/env python3
# SAFE CLEANUP — NO LOGIC / MODEL DELETION

from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]

FILES = [
    ROOT / "data" / "backtest_nifty_results.csv",
    ROOT / "data" / "backtest" / "nifty_backtest_results.csv",
    ROOT / "data" / "processed" / "ml" / "nifty_inference_features.parquet",
    ROOT / "data" / "processed" / "ml" / "nifty_ml_features_CANONICAL.parquet",
]

DIRS = [
    ROOT / "pipelines" / "__pycache__",
    ROOT / "pipelines" / "ml" / "__pycache__",
    ROOT / "configs" / "__pycache__",
]

print("🧹 SAFE CLEANUP START")

for f in FILES:
    if f.exists():
        f.unlink()
        print(f"✔ Deleted file: {f.relative_to(ROOT)}")

for d in DIRS:
    if d.exists():
        shutil.rmtree(d)
        print(f"✔ Removed dir: {d.relative_to(ROOT)}")

print("✅ SAFE CLEANUP COMPLETE")
