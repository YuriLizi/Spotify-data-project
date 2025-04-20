import pandas as pd
import pyarrow.parquet as pq
import argparse


def process_parquet_to_csv(input_file, output_file):
    # Read the Parquet file
    df = pd.read_parquet(input_file)

    # Group by episode_id and action, then count occurrences
    action_counts = df.groupby(['episode_id', 'action']).size().unstack(fill_value=0)

    # Ensure all required action columns exist (likes, listens, searches)
    for action in ['like', 'listen', 'search']:
        if action not in action_counts.columns:
            action_counts[action] = 0

    # Rename columns to match the required output
    action_counts = action_counts.rename(columns={
        'like': 'likes',
        'listen': 'listens',
        'search': 'searches'
    })

    # Calculate total actions
    action_counts['total_actions'] = action_counts['likes'] + action_counts['listens'] + action_counts['searches']

    # Reorder columns to match the required output
    action_counts = action_counts[['likes', 'listens', 'searches', 'total_actions']].reset_index()

    # Save to CSV
    action_counts.to_csv(output_file, index=False)
    print(f"Successfully processed data and saved to {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Parquet file and generate action counts CSV.')
    parser.add_argument('--input_file', help='Path to the input Parquet file')
    parser.add_argument('--output_file', help='Path to the output CSV file')

    args = parser.parse_args()

    process_parquet_to_csv(args.input_file, args.output_file)