from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[1]

PATTERNS = [
    "__pycache__",
    "*.pyc",
    "*.log",
]

print("🧹 Cleaning repository...")

for p in ROOT.rglob("*"):
    if p.is_dir() and p.name == "__pycache__":
        shutil.rmtree(p, ignore_errors=True)
        print("✔ Removed", p)

    if p.is_file() and p.suffix == ".pyc":
        p.unlink(missing_ok=True)

print("✅ CLEANUP COMPLETE")
