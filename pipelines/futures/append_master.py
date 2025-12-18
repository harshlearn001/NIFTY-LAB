from pathlib import Path
import pandas as pd

# --------------------------------------------------
# PATHS
# --------------------------------------------------
BASE = Path(r"H:\NIFTY-LAB")

DAILY_DIR  = BASE / "data" / "processed" / "daily" / "futures"
MASTER_PQ = BASE / "data" / "continuous" / "master_futures.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_futures.csv"

# --------------------------------------------------
# LOAD MASTER
# --------------------------------------------------
print("Loading master futures...")
master = pd.read_parquet(MASTER_PQ)

# --------------------------------------------------
# FIX MASTER SCHEMA (CRITICAL)
# --------------------------------------------------
# Rename old OI column if present
if "OI" in master.columns and "OPEN_INTEREST" not in master.columns:
    master = master.rename(columns={"OI": "OPEN_INTEREST"})

# Ensure OPEN_INTEREST exists
if "OPEN_INTEREST" not in master.columns:
    master["OPEN_INTEREST"] = pd.NA

# Force numeric
master["OPEN_INTEREST"] = pd.to_numeric(
    master["OPEN_INTEREST"], errors="coerce"
)

# --------------------------------------------------
# BACKFILL FROM DAILY FILES
# --------------------------------------------------
updates = 0

print("Backfilling OI from daily files...")

for f in DAILY_DIR.glob("FUT_NIFTY_*.parquet"):
    df = pd.read_parquet(f)

    # Normalize daily OI column
    if "OI" in df.columns:
        df = df.rename(columns={"OI": "OPEN_INTEREST"})

    if "OPEN_INTEREST" not in df.columns:
        continue

    df["OPEN_INTEREST"] = pd.to_numeric(
        df["OPEN_INTEREST"], errors="coerce"
    )

    for _, r in df.iterrows():
        mask = (
            (master["SYMBOL"] == r["SYMBOL"]) &
            (master["TRADE_DATE"] == r["TRADE_DATE"]) &
            (master["EXP_DATE"] == r["EXP_DATE"])
        )

        if mask.any():
            if master.loc[mask, "OPEN_INTEREST"].isna().any():
                master.loc[mask, "OPEN_INTEREST"] = r["OPEN_INTEREST"]
                updates += 1

# --------------------------------------------------
# SAVE (PARQUET + CSV)
# --------------------------------------------------
master.to_parquet(MASTER_PQ, index=False)
master.to_csv(MASTER_CSV, index=False)

print(f"✅ OI rows updated : {updates}")
print("MASTER FUTURES UPDATED")
print("DONE")
