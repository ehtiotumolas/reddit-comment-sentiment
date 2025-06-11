import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

DATA_DIR = "data"
OUTPUT_FILE = os.path.join(DATA_DIR, "all_comments_merged.parquet")

# Lists all .parquet files
files = [
    os.path.join(DATA_DIR, f)
    for f in os.listdir(DATA_DIR)
    if f.endswith(".parquet") and f != "all_comments_merged.parquet"
]

if not files:
    print("No Parquet files found.")
    exit(1)

dfs = []
for f in files:
    print(f"Loading {f} ...")
    dfs.append(pd.read_parquet(f))

print("Concatenating...")
df = pd.concat(dfs, ignore_index=True)

print("Dropping duplicates by comment id...")
df = df.drop_duplicates(subset=["id"])

print(f"Saving to {OUTPUT_FILE} ...")
pq.write_table(pa.Table.from_pandas(df), OUTPUT_FILE)
print("✅ Done! All comments are merged and deduplicated.")
