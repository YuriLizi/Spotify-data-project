"""
Data Validator Module

This module validates podcast data files after downloading from S3.
It performs checks on file integrity, format, and content.
"""

import os
import logging
import yaml
import pandas as pd
import xml.etree.ElementTree as ET
import json
import hashlib
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


def check_txt_file_format(file_path):
    """
    Validate format of a podcast stream txt file

    Args:
        file_path: Path to txt file

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Read the first few lines to check format
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [next(f) for _ in range(10)]

        # Check if file is empty
        if not lines:
            return False, "File is empty"

        # Check for expected columns (comma-separated values)
        if not any(',' in line for line in lines):
            return False, "File doesn't contain comma-separated values"

        # Try parsing with pandas to verify structure
        df = pd.read_csv(file_path, nrows=5)

        # Check for minimum expected columns
        required_columns = ['stream_id', 'user_id', 'timestamp']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            return False, f"Missing required columns: {', '.join(missing_columns)}"

        return True, ""

    except Exception as e:
        return False, f"Error validating file format: {str(e)}"


def check_xml_file_validity(xml_path):
    """
    Validate XML file structure

    Args:
        xml_path: Path to XML file

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Try parsing XML
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Check for RSS root element
        if root.tag != 'rss':
            return False, "Not a valid RSS feed (missing RSS root element)"

        # Check for channel element
        channel = root.find('channel')
        if channel is None:
            return False, "Missing channel element in RSS feed"

        # Check for essential podcast elements
        essential_elements = ['title', 'description', 'item']
        for element in essential_elements:
            if channel.find(element) is None:
                return False, f"Missing essential element '{element}' in RSS feed"

        # Check for items (episodes)
        items = channel.findall('item')
        if not items:
            return False, "RSS feed contains no episodes (items)"

        return True, ""

    except Exception as e:
        return False, f"Error validating XML: {str(e)}"


def generate_validation_report(extracted_dir, xml_path, output_dir):
    """
    Generate a validation report for all files

    Args:
        extracted_dir: Directory containing extracted txt files
        xml_path: Path to XML file
        output_dir: Directory to save validation report

    Returns:
        str: Path to validation report
    """
    report = {
        "validation_time": pd.Timestamp.now().isoformat(),
        "xml_file": {
            "path": xml_path,
            "size_bytes": os.path.getsize(xml_path),
            "md5_hash": hashlib.md5(open(xml_path, 'rb').read()).hexdigest()
        },
        "txt_files": [],
        "validation_results": {
            "xml_valid": False,
            "txt_files_valid": False,
            "overall_valid": False,
            "error_count": 0,
            "warning_count": 0
        }
    }

    # Validate XML file
    xml_valid, xml_error = check_xml_file_validity(xml_path)
    report["validation_results"]["xml_valid"] = xml_valid
    if not xml_valid:
        report["validation_results"]["error_count"] += 1
        report["xml_file"]["validation_error"] = xml_error

    # Validate txt files
    all_txt_valid = True
    txt_files = [f for f in os.listdir(extracted_dir) if f.endswith('.txt')]

    for txt_file in txt_files:
        file_path = os.path.join(extracted_dir, txt_file)
        file_info = {
            "filename": txt_file,
            "size_bytes": os.path.getsize(file_path),
            "md5_hash": hashlib.md5(open(file_path, 'rb').read()).hexdigest()
        }

        # Check file format
        is_valid, error_msg = check_txt_file_format(file_path)
        file_info["is_valid"] = is_valid

        if not is_valid:
            all_txt_valid = False
            report["validation_results"]["error_count"] += 1
            file_info["validation_error"] = error_msg

        report["txt_files"].append(file_info)

    # Set overall validation results
    report["validation_results"]["txt_files_valid"] = all_txt_valid
    report["validation_results"]["overall_valid"] = xml_valid and all_txt_valid

    # Save report
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, "validation_report.json")
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info(f"Validation report saved to {report_path}")
    logger.info(f"Validation result: {'PASSED' if report['validation_results']['overall_valid'] else 'FAILED'}")

    return report_path


def validate_raw_data(bronze_bucket, bronze_prefix, temp_dir):
    """
    Main function to validate downloaded podcast data

    Args:
        bronze_bucket: Bronze layer S3 bucket name
        bronze_prefix: Bronze layer prefix in S3
        temp_dir: Temporary directory containing downloaded files

    Returns:
        bool: True if validation passed, False otherwise
    """
    try:
        # Load configuration
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
        config_path = os.path.join(project_root, 'config', 'config.yaml')
        config = load_config(config_path)

        # Define paths
        extracted_dir = os.path.join(temp_dir, 'extracted')
        xml_path = os.path.join(temp_dir, config['files']['xml_file'])
        validation_dir = os.path.join(temp_dir, 'validation')

        # Generate validation report
        report_path = generate_validation_report(extracted_dir, xml_path, validation_dir)

        # Load report to check validation status
        with open(report_path, 'r') as f:
            report = json.load(f)

        return report["validation_results"]["overall_valid"]

    except Exception as e:
        logger.error(f"Error in validate_raw_data: {e}")
        raise


if __name__ == "__main__":
    # For standalone testing
    import argparse

    parser = argparse.ArgumentParser(description='Validate podcast data files')
    parser.add_argument('--config', default='../../config/config.yaml', help='Path to config file')
    parser.add_argument('--temp-dir', required=True, help='Temporary directory with downloaded files')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Execute validation process
    validate_raw_data(
        bronze_bucket=config['s3']['bronze_bucket'],
        bronze_prefix=config['s3']['bronze_prefix'],
        temp_dir=args.temp_dir
    )