"""
Data Cleaner Module

This module cleans raw podcast streaming data files from the bronze layer.
It processes txt files, handles various data quality issues, and prepares
the data for conversion to analytical formats.
"""

import os
import pandas as pd
import logging
import yaml
import glob
from datetime import datetime
import re
import json
import traceback
from pathlib import Path
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path):
    """
    Load configuration from YAML file

    Args:
        config_path: Path to config YAML file

    Returns:
        dict: Configuration values
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    logger.info(f"Loaded configuration from {config_path}")
    return config


def validate_timestamp(timestamp):
    """
    Validate and standardize timestamp format

    Args:
        timestamp: Timestamp string

    Returns:
        str: Standardized ISO timestamp or None if invalid
    """
    # Define common timestamp patterns
    patterns = [
        # ISO format: 2024-01-01T01:40:00
        (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', '%Y-%m-%dT%H:%M:%S'),
        # ISO with milliseconds: 2024-01-01T01:40:00.123
        (r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+$', '%Y-%m-%dT%H:%M:%S.%f'),
        # Date with space: 2024-01-01 01:40:00
        (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S'),
        # Unix timestamp (seconds since epoch)
        (r'^\d{10}$', None),
        # Unix timestamp (milliseconds since epoch)
        (r'^\d{13}$', None)
    ]

    if not timestamp or pd.isna(timestamp):
        return None

    timestamp = str(timestamp).strip()

    # Try each pattern
    for pattern, fmt in patterns:
        if re.match(pattern, timestamp):
            try:
                if fmt:
                    # Parse with specified format
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.isoformat()
                else:
                    # Handle Unix timestamps
                    if len(timestamp) == 10:  # seconds
                        dt = datetime.fromtimestamp(int(timestamp))
                        return dt.isoformat()
                    elif len(timestamp) == 13:  # milliseconds
                        dt = datetime.fromtimestamp(int(timestamp) / 1000)
                        return dt.isoformat()
            except (ValueError, OverflowError):
                continue

    # If no pattern matches, return None
    return None


def validate_uuid(uuid_str):
    """
    Validate if a string is a valid UUID

    Args:
        uuid_str: UUID string to validate

    Returns:
        str: Validated UUID or None if invalid
    """
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    if not uuid_str or pd.isna(uuid_str):
        return None

    uuid_str = str(uuid_str).strip().lower()

    if re.match(uuid_pattern, uuid_str):
        return uuid_str
    return None


def extract_episode_number(episode_id):
    """
    Extract only the number from episode ID

    Args:
        episode_id: Episode ID string like "episode-61"

    Returns:
        str: Extracted number or original string if no number found
    """
    if not episode_id or pd.isna(episode_id):
        return None

    episode_id = str(episode_id).strip()

    # Extract numbers from strings like "episode-61", "ep_123", etc.
    match = re.search(r'(\d+)', episode_id)
    if match:
        return match.group(1)

    return episode_id


def clean_file(input_path):
    """
    Clean a single podcast stream file

    Args:
        input_path: Path to input file

    Returns:
        pandas.DataFrame: Cleaned dataframe
    """
    # Infer column names based on file format
    # Format: timestamp|user_id|action|episode_id
    column_names = ['timestamp', 'user_id', 'action', 'episode_id']

    # Read file
    df = pd.read_csv(input_path, delimiter="|", header=None, names=column_names)
    original_row_count = len(df)
    logger.info(f"Read {original_row_count} rows from {os.path.basename(input_path)}")

    # Clean data
    # 1. Validate and standardize timestamps
    df['timestamp'] = df['timestamp'].apply(validate_timestamp)

    # 2. Validate UUIDs
    df['user_id'] = df['user_id'].apply(validate_uuid)

    # 3. Clean actions (lowercase and remove spaces)
    if 'action' in df.columns:
        df['action'] = df['action'].str.lower().str.strip()

    # 4. Clean episode IDs
    if 'episode_id' in df.columns:
        df['episode_id'] = df['episode_id'].apply(extract_episode_number)

    # 5. Filter out rows with invalid data
    df_clean = df.dropna(subset=['timestamp', 'user_id'])

    # 6. Add metadata columns
    df_clean['file_source'] = os.path.basename(input_path)
    df_clean['processed_at'] = datetime.now().isoformat()

    cleaned_row_count = len(df_clean)
    removed_row_count = original_row_count - cleaned_row_count

    logger.info(f"Cleaned file {os.path.basename(input_path)}: "
                f"{cleaned_row_count} rows retained, {removed_row_count} rows removed")

    return df_clean


def clean_data(input_dir, output_dir, combined_output=True):
    """
    Clean all txt files in the input directory and optionally create a combined CSV

    Args:
        input_dir: Directory containing raw txt files
        output_dir: Directory to save cleaned files
        combined_output: Whether to generate a combined CSV file

    Returns:
        dict: Summary of cleaning process
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Get all txt files in input directory
        txt_files = glob.glob(os.path.join(input_dir, "*.txt"))

        if not txt_files:
            logger.warning(f"No txt files found in {input_dir}")
            return {"files_processed": 0, "rows_processed": 0}

        logger.info(f"Found {len(txt_files)} txt files to process")

        total_rows = 0
        successful_files = 0
        failed_files = []
        all_cleaned_data = pd.DataFrame()

        # Process each file
        for input_path in txt_files:
            try:
                file_name = os.path.basename(input_path)
                output_path = os.path.join(output_dir, f"clean_{file_name}")

                # Clean file
                df_clean = clean_file(input_path)

                # Save individual cleaned file
                df_clean.to_csv(output_path, index=False)

                # Collect for combined output
                if combined_output:
                    all_cleaned_data = pd.concat([all_cleaned_data, df_clean], ignore_index=True)

                total_rows += len(df_clean)
                successful_files += 1

            except Exception as e:
                failed_files.append(os.path.basename(input_path))
                stack_trace = traceback.format_exc()
                logger.error(f"Failed to process file {input_path}: {e}\n{stack_trace}")

        # Save combined CSV if requested
        if combined_output and not all_cleaned_data.empty:
            combined_path = os.path.join(output_dir, "combined_podcast_data.csv")
            all_cleaned_data.to_csv(combined_path, index=False)
            logger.info(f"Saved combined data to {combined_path} ({len(all_cleaned_data)} total rows)")

        # Generate summary
        summary = {
            "files_processed": successful_files,
            "total_files": len(txt_files),
            "rows_processed": total_rows,
            "failed_files": failed_files,
            "timestamp": datetime.now().isoformat(),
            "combined_output_created": combined_output and not all_cleaned_data.empty
        }

        # Save summary
        summary_path = os.path.join(output_dir, "cleaning_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Processed {successful_files}/{len(txt_files)} files, {total_rows} total rows")
        if failed_files:
            logger.warning(f"Failed to process {len(failed_files)} files: {', '.join(failed_files)}")

        return summary

    except Exception as e:
        stack_trace = traceback.format_exc()
        logger.error(f"Error in clean_data: {e}\n{stack_trace}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean podcast streaming data')
    parser.add_argument('--config', default='../../config/config.yaml', help='Path to config file')
    parser.add_argument('--input-dir', required=True, help='Directory containing raw txt files')
    parser.add_argument('--output-dir', required=True, help='Directory to save cleaned files')
    parser.add_argument('--no-combined', action='store_true', help='Skip creating combined CSV output')
    args = parser.parse_args()

    try:
        # Load configuration if needed
        try:
            config = load_config(args.config)
        except Exception:
            logger.warning("Could not load config file. Using command line arguments only.")

        # Execute cleaning process
        clean_data(args.input_dir, args.output_dir, combined_output=not args.no_combined)
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}")
        traceback.print_exc()
        exit(1)