import sys
import os
import pandas as pd
import orjson
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor


def serialize_chunk(records):
    # Serialises a list of dicts into JSON lines bytes
    return b"".join(orjson.dumps(rec) + b"\n" for rec in records)


def parquet_to_json(parquet_path, output_path, chunk_size=100_000, workers=4):
    # Reads the parquet into a DataFrame
    df = pd.read_parquet(parquet_path)
    total = len(df)

    with open(output_path, "wb") as f, tqdm(
        total=total, desc="Writing JSON", unit="rows"
    ) as pbar, ProcessPoolExecutor(max_workers=workers) as pool:

        futures = []
        for start in range(0, total, chunk_size):
            chunk = df.iloc[start : start + chunk_size].to_dict(orient="records")
            futures.append(pool.submit(serialize_chunk, chunk))

        # As each chunk completes, write it out and update progress
        for future in futures:
            data = future.result()
            f.write(data)
            # Estimate rows from byte size roughly: use chunk_size or track actual counts
            pbar.update(chunk_size)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    if not path or not path.lower().endswith(".parquet"):
        print("Provide a .parquet file as argument.")
        sys.exit(1)
    out = os.path.splitext(path)[0] + ".jsonl"
    parquet_to_json(path, out)
    print(f"Done → {out}")
