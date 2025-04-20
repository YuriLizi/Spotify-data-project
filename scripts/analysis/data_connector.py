import argparse
import pandas as pd


def combine_csvs(podcast_csv, stats_csv, output_csv):
    """
    Combine podcast metadata CSV with statistics CSV based on episode number.

    Args:
        podcast_csv (str): Path to the podcast metadata CSV file
        stats_csv (str): Path to the statistics CSV file
        output_csv (str): Path for the output combined CSV file
    """
    try:
        # Read both CSV files
        podcast_df = pd.read_csv(podcast_csv)
        stats_df = pd.read_csv(stats_csv)

        # Rename episode_id to episode_number in stats for consistent merging
        stats_df = stats_df.rename(columns={'episode_id': 'episode_number'})

        # Merge the dataframes on episode_number
        combined_df = pd.merge(podcast_df, stats_df, on='episode_number', how='left')

        # Save the combined dataframe to a new CSV file
        combined_df.to_csv(output_csv, index=False)

        print(f"Successfully combined files. Output saved to {output_csv}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Combine podcast metadata with statistics based on episode number.')

    # Add arguments
    parser.add_argument('--podcast', required=True, help='Path to podcast metadata CSV file')
    parser.add_argument('--stats', required=True, help='Path to statistics CSV file')
    parser.add_argument('--output', required=True, help='Path for output combined CSV file')

    # Parse arguments
    args = parser.parse_args()

    # Call the combine function
    combine_csvs(args.podcast, args.stats, args.output)