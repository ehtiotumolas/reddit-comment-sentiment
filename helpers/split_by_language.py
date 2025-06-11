import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_FILE = "data/all_comments.parquet"
EN_FILE = "data/comments_en.parquet"
NON_EN_FILE = "data/comments_non_en.parquet"


def main():
    print(f"Loading {INPUT_FILE} ...")
    df = pd.read_parquet(INPUT_FILE)
    print(f"Total records: {len(df)}")

    # Ensure 'language' column exists
    if "language" not in df.columns:
        raise ValueError(
            "No 'language' column found. Please run language detection first."
        )

    # Split based on language
    df_en = df[df["language"] == "en"].copy()
    df_non_en = df[df["language"] != "en"].copy()

    print(f"English comments: {len(df_en)}")
    print(f"Non-English comments: {len(df_non_en)}")

    # Save
    pq.write_table(pa.Table.from_pandas(df_en), EN_FILE)
    pq.write_table(pa.Table.from_pandas(df_non_en), NON_EN_FILE)

    print(
        f"Split completed. Saved English to '{EN_FILE}' and non-English to '{NON_EN_FILE}'."
    )


if __name__ == "__main__":
    main()
