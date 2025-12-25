from pathlib import Path
import pandas as pd

BASE = Path(r"H:\NIFTY-LAB")
MASTER_PQ  = BASE / "data" / "continuous" / "master_futures.parquet"
MASTER_CSV = BASE / "data" / "continuous" / "master_futures.csv"

print("Loading master futures...")
df = pd.read_parquet(MASTER_PQ)

before = len(df)

# Drop fully empty rows
df = df.dropna(subset=["TRADE_DATE", "EXP_DATE", "SYMBOL"], how="any")

after = len(df)

df = df.sort_values(["TRADE_DATE", "EXP_DATE"]).reset_index(drop=True)

df.to_parquet(MASTER_PQ, index=False)
df.to_csv(MASTER_CSV, index=False)

print(f"Removed {before - after} empty rows")
print("MASTER CLEANED")
