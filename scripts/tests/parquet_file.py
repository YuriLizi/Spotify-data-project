import pandas as pd
import pyarrow.parquet as pq
import argparse
import os

def view_parquet(file_path):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return

    print(f" Reading Parquet file: {file_path}")

    # Load metadata
    parquet_file = pq.ParquetFile(file_path)

    print("\nSchema:")
    print(parquet_file.schema)

    print(f"\n Number of Row Groups: {parquet_file.num_row_groups}")
    print(f" Estimated Total Rows: {parquet_file.metadata.num_rows}")
    print(f" Columns: {[name for name in parquet_file.schema.names]}")

    print("\n First few rows:")
    try:
        df = pd.read_parquet(file_path)
        print(df.head())
    except Exception as e:
        print("️ Could not read full file with Pandas.")
        print("Error:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="View info about a Parquet file")
    parser.add_argument("--file_path", type=str, help="Path to the Parquet file")

    args = parser.parse_args()
    view_parquet(args.file_path)
