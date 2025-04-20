"""
S3 Uploader Module

This module handles uploading processed podcast data files to S3 buckets.
It supports uploading to different data layers (bronze, silver, gold).
"""

import boto3
import os
import logging
import yaml
import glob
from pathlib import Path

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
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise


def get_s3_client(credentials_path):
    """
    Create S3 client using credentials from YAML file

    Args:
        credentials_path: Path to credentials YAML file

    Returns:
        boto3.client: Configured S3 client
    """
    try:
        with open(credentials_path, 'r') as file:
            credentials = yaml.safe_load(file)

        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['aws']['access_key_id'],
            aws_secret_access_key=credentials['aws']['secret_access_key'],
            region_name=credentials['aws']['region']
        )
        logger.info("Successfully created S3 client")
        return s3_client
    except Exception as e:
        logger.error(f"Error creating S3 client: {e}")
        raise


def upload_directory_to_s3(s3_client, local_dir, bucket, prefix, file_pattern="*"):
    """
    Upload all files in a directory to S3

    Args:
        s3_client: Configured S3 client
        local_dir: Local directory containing files to upload
        bucket: Destination S3 bucket
        prefix: S3 key prefix (folder)
        file_pattern: File pattern to match (default: all files)

    Returns:
        int: Number of files uploaded
    """
    try:
        # Ensure the directory exists
        if not os.path.exists(local_dir):
            logger.error(f"Local directory does not exist: {local_dir}")
            return 0

        # Get files matching pattern
        search_pattern = os.path.join(local_dir, file_pattern)
        files = glob.glob(search_pattern)

        if not files:
            logger.warning(f"No files found matching pattern '{file_pattern}' in {local_dir}")
            return 0

        # Upload each file
        upload_count = 0
        for file_path in files:
            if os.path.isfile(file_path):
                file_name = os.path.basename(file_path)
                s3_key = f"{prefix.rstrip('/')}/{file_name}"

                logger.info(f"Uploading {file_name} to s3://{bucket}/{s3_key}")
                s3_client.upload_file(file_path, bucket, s3_key)
                upload_count += 1

        logger.info(f"Successfully uploaded {upload_count} files to S3")
        return upload_count

    except Exception as e:
        logger.error(f"Error uploading files to S3: {e}")
        raise


def upload_to_data_layer(source_dir, bucket, prefix, layer_name, file_pattern="*"):
    """
    Upload files to a specific data layer in S3

    Args:
        source_dir: Local directory containing files to upload
        bucket: Destination S3 bucket
        prefix: S3 key prefix (folder)
        layer_name: Name of data layer (bronze, silver, gold)
        file_pattern: File pattern to match

    Returns:
        int: Number of files uploaded
    """
    try:
        # Load credentials
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        #credentials_path = os.path.join(project_root, 'config', 'credentials.yaml')
        credentials_path = "/home/cortica/2nd_degree/work_search_projects/spotify_project_py/configs/credentials.yaml "

        # Get S3 client
        s3_client = get_s3_client(credentials_path)

        logger.info(f"Starting upload to {layer_name} layer")

        # Upload files
        uploaded = upload_directory_to_s3(
            s3_client=s3_client,
            local_dir=source_dir,
            bucket=bucket,
            prefix=prefix,
            file_pattern=file_pattern
        )

        logger.info(f"Completed upload to {layer_name} layer: {uploaded} files")
        return uploaded

    except Exception as e:
        logger.error(f"Error in upload_to_data_layer: {e}")
        raise


def upload_to_bronze_layer(source_dir, bucket, prefix, file_pattern="*"):
    """
    Upload files to bronze data layer

    Args:
        source_dir: Source directory with files
        bucket: Destination S3 bucket
        prefix: S3 prefix (folder)
        file_pattern: File pattern to match

    Returns:
        int: Number of files uploaded
    """
    return upload_to_data_layer(source_dir, bucket, prefix, "bronze", file_pattern)


def upload_to_silver_layer(source_dir, bucket, prefix, file_pattern="*"):
    """
    Upload files to silver data layer

    Args:
        source_dir: Source directory with files
        bucket: Destination S3 bucket
        prefix: S3 prefix (folder)
        file_pattern: File pattern to match

    Returns:
        int: Number of files uploaded
    """
    return upload_to_data_layer(source_dir, bucket, prefix, "silver", file_pattern)


def upload_to_gold_layer(source_dir, bucket, prefix, file_pattern="*"):
    """
    Upload files to gold data layer

    Args:
        source_dir: Source directory with files
        bucket: Destination S3 bucket
        prefix: S3 prefix (folder)
        file_pattern: File pattern to match

    Returns:
        int: Number of files uploaded
    """
    return upload_to_data_layer(source_dir, bucket, prefix, "gold", file_pattern)


if __name__ == "__main__":
    # For standalone testing
    import argparse

    parser = argparse.ArgumentParser(description='Upload podcast data to S3')
    parser.add_argument('--config', default='../../config/config.yaml', help='Path to config file')
    parser.add_argument('--source-dir', required=True, help='Source directory with files to upload')
    parser.add_argument('--layer', choices=['bronze', 'silver', 'gold'], required=True,
                        help='Data layer to upload to')
    parser.add_argument('--pattern', default='*', help='File pattern to match')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Execute upload process based on specified layer
    if args.layer == 'bronze':
        upload_to_bronze_layer(
            source_dir=args.source_dir,
            bucket=config['s3']['bronze_bucket'],
            prefix=config['s3']['bronze_prefix'],
            file_pattern=args.pattern
        )
    elif args.layer == 'silver':
        upload_to_silver_layer(
            source_dir=args.source_dir,
            bucket=config['s3']['silver_bucket'],
            prefix=config['s3']['silver_prefix'],
            file_pattern=args.pattern
        )
    elif args.layer == 'gold':
        upload_to_gold_layer(
            source_dir=args.source_dir,
            bucket=config['s3']['gold_bucket'],
            prefix=config['s3']['gold_prefix'],
            file_pattern=args.pattern
        )