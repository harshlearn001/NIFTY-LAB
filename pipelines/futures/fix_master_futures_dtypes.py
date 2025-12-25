from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")

MASTER_PQ  = BASE / "data" / "continuous" / "master_futures.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_futures.csv"

print("Loading master futures...")
df = pd.read_parquet(MASTER_PQ)

# --------------------------------------------------
# FORCE INTEGER TYPES (NULLABLE)
# --------------------------------------------------
for col in ["VOLUME", "OPEN_INTEREST"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

# --------------------------------------------------
# SAVE BACK
# --------------------------------------------------
df.to_parquet(MASTER_PQ, index=False)
df.to_csv(MASTER_CSV, index=False)

print("MASTER FUTURES DTYPES FIXED")
print(df.dtypes)
print("DONE")
