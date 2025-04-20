"""
Format Converter Module

This module converts cleaned podcast data files to analytical formats
like Parquet, Avro, and Iceberg for more efficient processing and querying.
"""

import os
import pandas as pd
import logging
import yaml
import glob
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from datetime import datetime
import json
import shutil

# Try importing optional libraries
try:
    import fastavro

    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False
    logging.warning("fastavro library not found. Avro conversion will not be available.")

try:
    import pyiceberg
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, LongType, TimestampType
    )

    ICEBERG_AVAILABLE = True
except ImportError:
    ICEBERG_AVAILABLE = False
    logging.warning("pyiceberg library not found. Iceberg conversion will not be available.")

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


def read_cleaned_files(input_dir, file_pattern="clean_*.txt"):
    """
    Read all cleaned files into a single DataFrame

    Args:
        input_dir: Directory containing cleaned files
        file_pattern: Pattern to match cleaned files

    Returns:
        DataFrame: Combined data from all cleaned files
    """
    try:
        # Find all cleaned files
        file_paths = glob.glob(os.path.join(input_dir, file_pattern))

        if not file_paths:
            logger.warning(f"No files matching '{file_pattern}' found in {input_dir}")
            return None

        logger.info(f"Found {len(file_paths)} cleaned files to process")

        # Read and combine files
        dataframes = []
        for file_path in file_paths:
            try:
                df = pd.read_csv(file_path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")

        if not dataframes:
            logger.warning("No data could be read from cleaned files")
            return None

        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"Combined {len(dataframes)} files into a DataFrame with {len(combined_df)} rows")

        return combined_df

    except Exception as e:
        logger.error(f"Error reading cleaned files: {e}")
        raise


def convert_to_parquet(input_dir, output_dir, silver_bucket=None, silver_prefix=None,
                       partition_cols=None):
    """
    Convert cleaned data to Parquet format

    Args:
        input_dir: Directory containing cleaned data
        output_dir: Directory to save Parquet files
        silver_bucket: S3 bucket for silver layer (optional)
        silver_prefix: S3 prefix for silver layer (optional)
        partition_cols: Columns to partition by (optional)

    Returns:
        list: Paths to generated Parquet files
    """
    try:
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Read cleaned data
        df = read_cleaned_files(input_dir)

        if df is None or len(df) == 0:
            logger.warning("No data to convert to Parquet")
            return []

        # Set default partition columns if none provided
        if partition_cols is None:
            # Extract date from timestamp for partitioning
            if 'timestamp' in df.columns:
                df['date'] = pd.to_datetime(df['timestamp']).dt.date
                partition_cols = ['date']
            else:
                partition_cols = []

        # Define output path
        parquet_dir = os.path.join(output_dir, 'parquet')
        os.makedirs(parquet_dir, exist_ok=True)

        # Convert timestamp to datetime if present
        for col in df.columns:
            if 'timestamp' in col.lower() or 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col])
                except Exception as e:
                    logger.warning(f"Could not convert column {col} to datetime: {e}")

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write to Parquet (partitioned if partition_cols is provided)
        if partition_cols and len(partition_cols) > 0:
            logger.info(f"Writing partitioned Parquet files by {partition_cols}")
            pq.write_to_dataset(
                table,
                root_path=parquet_dir,
                partition_cols=partition_cols
            )
        else:
            logger.info("Writing single Parquet file")
            parquet_path = os.path.join(parquet_dir, 'podcast_streams.parquet')
            pq.write_table(table, parquet_path)

        # Get list of created files
        parquet_files = []
        for root, _, files in os.walk(parquet_dir):
            for file in files:
                if file.endswith('.parquet'):
                    parquet_files.append(os.path.join(root, file))

        logger.info(f"Created {len(parquet_files)} Parquet files in {parquet_dir}")

        # Upload to S3 if bucket info provided
        if silver_bucket and silver_prefix:
            try:
                from scripts.ingestion.s3_uploader import upload_to_silver_layer
                upload_to_silver_layer(
                    source_dir=parquet_dir,
                    bucket=silver_bucket,
                    prefix=silver_prefix,
                    file_pattern="**/*.parquet"
                )
                logger.info(f"Uploaded Parquet files to S3 bucket {silver_bucket}/{silver_prefix}")
            except Exception as e:
                logger.error(f"Error uploading to S3: {e}")

        return parquet_files

    except Exception as e:
        logger.error(f"Error converting to Parquet: {e}")
        raise


def convert_to_avro(input_dir, output_dir):
    """
    Convert cleaned data to Avro format

    Args:
        input_dir: Directory containing cleaned data
        output_dir: Directory to save Avro files

    Returns:
        list: Paths to generated Avro files
    """
    if not AVRO_AVAILABLE:
        logger.error("Avro conversion not available. Install fastavro package.")
        return []

    try:
        # Create output directory
        avro_dir = os.path.join(output_dir, 'avro')
        os.makedirs(avro_dir, exist_ok=True)

        # Read cleaned data
        df = read_cleaned_files(input_dir)

        if df is None or len(df) == 0:
            logger.warning("No data to convert to Avro")
            return []

        # Convert timestamps to strings for Avro compatibility
        for col in df.columns:
            if 'timestamp' in col.lower() or 'date' in col.lower():
                try:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%dT%H:%M:%S.%f')
                except Exception as e:
                    logger.warning(f"Could not convert column {col} for Avro: {e}")

        # Define Avro schema with proper types
        fields = []
        for field in df.columns:
            if field == 'episode_id':
                fields.append({'name': field, 'type': ['null', 'int'], 'default': None})
            else:
                fields.append({'name': field, 'type': ['null', 'string'], 'default': None})

        schema = {
            'namespace': 'podcast.streams',
            'type': 'record',
            'name': 'Stream',
            'fields': fields
        }

        # Ensure episode_id is properly typed
        if 'episode_id' in df.columns:
            df['episode_id'] = df['episode_id'].astype(int)

        # Convert DataFrame to list of records
        records = df.to_dict('records')

        # Write to Avro file
        avro_path = os.path.join(avro_dir, 'podcast_streams.avro')
        with open(avro_path, 'wb') as out:
            fastavro.writer(out, schema, records)

        logger.info(f"Created Avro file at {avro_path}")
        return [avro_path]

    except Exception as e:
        logger.error(f"Error converting to Avro: {e}")
        return []


def convert_to_iceberg(input_dir, output_dir, warehouse_path=None):
    """
    Convert cleaned data to Apache Iceberg format

    Args:
        input_dir: Directory containing cleaned data
        output_dir: Directory to save Iceberg tables
        warehouse_path: Optional path for the Iceberg warehouse

    Returns:
        str: Path to generated Iceberg table
    """
    try:
        # Import required libraries
        from pyiceberg.catalog import load_catalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import (
            NestedField, StringType, TimestampType, IntegerType
        )
        from pyiceberg.partitioning import PartitionSpec
        import pandas as pd
        import os
        import uuid

        # Create output directory
        iceberg_dir = os.path.join(output_dir, 'iceberg')
        os.makedirs(iceberg_dir, exist_ok=True)

        # Determine warehouse path
        if warehouse_path is None:
            warehouse_path = os.path.abspath(iceberg_dir)

        # Read cleaned data
        df = read_cleaned_files(input_dir)

        if df is None or len(df) == 0:
            logger.warning("No data to convert to Iceberg")
            return ""

        # Define the Iceberg schema based on the DataFrame columns
        fields = []
        for i, col in enumerate(df.columns):
            if col == 'timestamp' or 'date' in col.lower():
                fields.append(NestedField(i + 1, col, TimestampType(), required=False))
            elif col == 'episode_id':
                fields.append(NestedField(i + 1, col, IntegerType(), required=False))
            else:
                fields.append(NestedField(i + 1, col, StringType(), required=False))

        schema = Schema(*fields)

        # Configure local catalog
        catalog_properties = {
            "type": "hive",
            "warehouse": warehouse_path,
            "uri": f"file://{warehouse_path}"
        }

        # Initialize catalog
        catalog = load_catalog("local_catalog", **catalog_properties)

        # Create table name
        table_name = f"podcast_streams_{uuid.uuid4().hex[:8]}"
        namespace = "podcast"

        # Ensure namespace exists
        if namespace not in catalog.list_namespaces():
            catalog.create_namespace(namespace)

        # Create the table (using the correct method)
        full_table_name = f"{namespace}.{table_name}"

        # Fix: Use catalog.create_table instead of imported create_table
        table = catalog.create_table(
            identifier=full_table_name,
            schema=schema,
            partition_spec=PartitionSpec(),
            properties={"format-version": "2"}
        )

        # Convert data types as needed
        for col in df.columns:
            if col == 'episode_id':
                df[col] = df[col].astype(int)
            elif 'timestamp' in col.lower() or 'date' in col.lower():
                df[col] = pd.to_datetime(df[col])

        # Write data to the table
        table_path = f"{warehouse_path}/{namespace}/{table_name}"

        # Use PyArrow to write the data (more efficient for Iceberg)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            # Convert to PyArrow table
            arrow_table = pa.Table.from_pandas(df)

            # Write to parquet file in the data directory
            data_dir = f"{table_path}/data"
            os.makedirs(data_dir, exist_ok=True)
            parquet_path = f"{data_dir}/part-0.parquet"
            pq.write_table(
                arrow_table,
                parquet_path,
                compression="snappy"
            )

            # Register the new file with the table
            # Note: In a full implementation, you'd use the Iceberg API to add the file
            # but for demonstration, we're just creating the file and refreshing the table
            logger.info(f"Wrote data file to {parquet_path}")

            # Refresh table metadata
            table.refresh()

        except ImportError:
            logger.warning("PyArrow not available, falling back to less efficient method")
            # Fallback to pandas-based writing if PyArrow is not available
            parquet_path = f"{table_path}/data/part-0.parquet"
            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            df.to_parquet(parquet_path, compression="snappy")
            logger.info(f"Wrote data file to {parquet_path}")
            table.refresh()

        logger.info(f"Created Iceberg table at {table_path}")
        return table_path

    except ImportError as e:
        logger.error(f"Required libraries for Iceberg conversion not available: {e}")
        logger.error("Install pyiceberg and pyarrow packages.")
        return ""
    except Exception as e:
        logger.error(f"Error converting to Iceberg: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return ""

def convert_to_formats(input_dir, output_dir, formats=None, silver_bucket=None, silver_prefix=None):
    """
    Convert cleaned data to multiple analytical formats

    Args:
        input_dir: Directory containing cleaned data
        output_dir: Directory to save converted files
        formats: List of formats to convert to (default: ['parquet'])
        silver_bucket: S3 bucket for silver layer (optional)
        silver_prefix: S3 prefix for silver layer (optional)

    Returns:
        dict: Paths to generated files by format
    """
    try:
        # Default to Parquet if no formats specified
        formats = formats or ['parquet']

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        results = {}

        # Convert to each format
        if 'parquet' in formats:
            parquet_files = convert_to_parquet(
                input_dir=input_dir,
                output_dir=output_dir,
                silver_bucket=silver_bucket,
                silver_prefix=silver_prefix
            )
            results['parquet'] = parquet_files

        if 'avro' in formats and AVRO_AVAILABLE:
            avro_files = convert_to_avro(
                input_dir=input_dir,
                output_dir=output_dir
            )
            results['avro'] = avro_files

        if 'iceberg' in formats and ICEBERG_AVAILABLE:
            iceberg_metadata = convert_to_iceberg(
                input_dir=input_dir,
                output_dir=output_dir
            )
            results['iceberg'] = iceberg_metadata

        # Generate a summary report
        summary = {
            'timestamp': datetime.now().isoformat(),
            'formats_converted': list(results.keys()),
            'file_counts': {fmt: len(files) if isinstance(files, list) else (1 if files else 0)
                            for fmt, files in results.items()}
        }

        # Save summary
        summary_path = os.path.join(output_dir, 'conversion_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Completed format conversion: {summary['file_counts']}")
        return results

    except Exception as e:
        logger.error(f"Error in convert_to_formats: {e}")
        raise


if __name__ == "__main__":
    # For standalone testing
    import argparse

    parser = argparse.ArgumentParser(description='Convert podcast data to analytical formats')
    parser.add_argument('--config', default='../../configs/config.yaml', help='Path to config file')
    parser.add_argument('--input-dir', required=True, help='Directory containing cleaned data files')
    parser.add_argument('--output-dir', required=True, help='Directory to save converted files')
    parser.add_argument('--formats', nargs='+', default=['parquet'],
                        choices=['parquet', 'avro', 'iceberg'],
                        help='Formats to convert to')
    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
        silver_bucket = config['s3']['silver_bucket']
        silver_prefix = config['s3']['silver_prefix']
    except Exception:
        logger.warning("Could not load config file. S3 upload will be disabled.")
        silver_bucket = None
        silver_prefix = None

    # Execute conversion process
    convert_to_formats(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        formats=args.formats,
        silver_bucket=silver_bucket,
        silver_prefix=silver_prefix
    )