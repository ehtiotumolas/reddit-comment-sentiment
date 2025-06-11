import os
from typing import List, Tuple

import pyarrow.parquet as pq

DATA_DIR = "data"
DATA_DIR2 = "data/comments_parquet"
DATA_DIR3 = "data/users_parquet"

# Prefix for comment Parquet files
COMMENTS_PARQUET_PREFIX = "comments_"
# Prefix for user Parquet files
USERS_PARQUET_PREFIX = "users_"


def count_rows_by_prefix(data_dirs: List[str]) -> Tuple[int, int]:
    """
    Counts the number of rows in Parquet files across multiple directories.
    User rows are identified by the user file prefix, comment rows by the comment file prefix.
    Returns a tuple: (user_rows, comment_rows).
    """
    user_rows = 0
    comment_rows = 0

    # Iterates over each specified directory
    for data_dir in data_dirs:
        # Skips directories that do not exist
        if not os.path.isdir(data_dir):
            continue

        # Iterates over each file in the directory
        for fn in os.listdir(data_dir):
            # Skips files that are not Parquet
            if not fn.endswith(".parquet"):
                continue
            path = os.path.join(data_dir, fn)
            # Reads only file metadata to count rows without loading full data
            pf = pq.ParquetFile(path)
            num_rows = pf.metadata.num_rows

            # Increments user row count if filename matches user prefix
            if fn.startswith(USERS_PARQUET_PREFIX):
                user_rows += num_rows
            # Increments comment row count if filename matches comment prefix
            elif fn.startswith(COMMENTS_PARQUET_PREFIX):
                comment_rows += num_rows

    return user_rows, comment_rows


if __name__ == "__main__":
    # Aggregates counts from all data directories
    users_count, comments_count = count_rows_by_prefix([DATA_DIR, DATA_DIR2, DATA_DIR3])

    # Prints the aggregated results with formatted numbers
    print(f"📊 Total user‐rows:    {users_count:,}")
    print(f"📊 Total comment‐rows: {comments_count:,}")
