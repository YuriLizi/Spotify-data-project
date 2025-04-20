"""
S3 Downloader Module

This module handles downloading podcast data files from S3 buckets.
It retrieves the necessary zip and XML files and extracts them for processing.
"""

import boto3
import os
import zipfile
import logging
import yaml
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


def ensure_dir(directory):
    """
    Create directory if it doesn't exist

    Args:
        directory: Directory path to create
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")


def download_files_from_s3(source_bucket, bronze_bucket, source_prefix, bronze_prefix,
                           local_temp_dir, zip_file_name, xml_file_name):
    """
    Download podcast data files from S3 and extract them

    Args:
        source_bucket: Source S3 bucket name
        bronze_bucket: Bronze layer S3 bucket name
        source_prefix: Source prefix in S3
        bronze_prefix: Bronze layer prefix in S3
        local_temp_dir: Local temporary directory
        zip_file_name: Name of zip file to download
        xml_file_name: Name of XML file to download

    Returns:
        tuple: (extract_dir, xml_path) paths to extracted files and XML
    """
    try:
        # Load credentials and configuration
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        credentials_path = "/home/cortica/2nd_degree/work_search_projects/spotify_project_py/configs/credentials.yaml "

       # credentials_path = os.path.join(project_root, 'configs', 'credentials.yaml')

        # Get S3 client
        s3_client = get_s3_client(credentials_path)

        # Ensure temporary directory exists
        ensure_dir(local_temp_dir)

        # Download ZIP file
        zip_path = os.path.join(local_temp_dir, zip_file_name)
        logger.info(f"Downloading {zip_file_name} from S3...")
        s3_client.download_file(
            source_bucket,
            f"{source_prefix}{zip_file_name}",
            zip_path
        )
        logger.info(f"Successfully downloaded {zip_file_name}")

        # Download XML file
        xml_path = os.path.join(local_temp_dir, xml_file_name)
        logger.info(f"Downloading {xml_file_name} from S3...")
        s3_client.download_file(
            source_bucket,
            f"{source_prefix}{xml_file_name}",
            xml_path
        )
        logger.info(f"Successfully downloaded {xml_file_name}")

        # Extract files from zip
        extract_dir = os.path.join(local_temp_dir, 'extracted')
        ensure_dir(extract_dir)

        logger.info(f"Extracting {zip_file_name}...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Filter to extract only txt files
            txt_files = [f for f in zip_ref.namelist() if f.endswith('.txt')]
            for txt_file in txt_files:
                zip_ref.extract(txt_file, extract_dir)

        logger.info(f"Extracted {len(txt_files)} txt files to {extract_dir}")

        return extract_dir, xml_path

    except Exception as e:
        logger.error(f"Error in download_files_from_s3: {e}")
        raise


if __name__ == "__main__":
    # For standalone testing
    import argparse

    parser = argparse.ArgumentParser(description='Download podcast data from S3')
    parser.add_argument('--config', default='../../config/config.yaml', help='Path to config file')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Execute download process
    download_files_from_s3(
        source_bucket=config['s3']['source_bucket'],
        bronze_bucket=config['s3']['bronze_bucket'],
        source_prefix=config['s3']['source_prefix'],
        bronze_prefix=config['s3']['bronze_prefix'],
        local_temp_dir=config['processing']['temp_dir'],
        zip_file_name=config['files']['zip_file'],
        xml_file_name=config['files']['xml_file']
    )