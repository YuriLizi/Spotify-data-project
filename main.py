#!/usr/bin/env python3
import os
import argparse
import logging
import yaml
from datetime import datetime

# Import functions from the different modules
from scripts.ingestion.s3_downloader import download_files_from_s3
from scripts.ingestion.s3_uploader import (
    upload_to_bronze_layer,
    upload_to_silver_layer,
    upload_to_gold_layer
)
from scripts.processing.rss_parser import parse_rss_feed
from scripts.processing.data_cleaner import clean_data
from scripts.processing.format_converter import convert_to_formats
from scripts.metadata.setup_trino import setup_local_trino_for_parquet
from scripts.sql.trino_queries_flow import execute_trino_query, split_sql_queries
from scripts.analysis.data_connector import combine_csvs
from scripts.analysis.analyze_podcasts import analyze_podcasts

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SpotifyPipeline")


def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_directories(directories):
    """Create directories if they don't exist"""
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Created directory: {directory}")


def run_trino_queries(query_file, host, port, user, catalog, schema, output_dir):
    """Execute Trino queries from a file and return the last result"""
    # Read queries from file
    with open("/home/cortica/2nd_degree/work_search_projects/spotify_project_py/scripts/sql/queries.sql", 'r') as f:
        sql_content = f.read()
    #sql_content = "/home/cortica/2nd_degree/work_search_projects/spotify_project_py/scripts/sql/queries.sql"
    queries = split_sql_queries(sql_content)

    # Execute each query
    last_result = None
    for i, query in enumerate(queries, 1):
        logger.info(f"Executing Query {i}:\n{query}")
        try:
            results_df = execute_trino_query(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema,
                sql_query=query
            )

            logger.info(f"Query {i} executed successfully")

            # Save to file
            output_file = os.path.join(output_dir, f"query_result_{i}.csv")
            results_df.to_csv(output_file, index=False)
            logger.info(f"Results saved to {output_file}")

            # Keep track of the last result
            last_result = results_df

            # Save the last query result to a specific file for the next stage
            if i == len(queries):
                final_output = os.path.join(output_dir, "final_query_result.csv")
                results_df.to_csv(final_output, index=False)
                logger.info(f"Final query result saved to {final_output}")

        except Exception as e:
            logger.error(f"Error executing Query {i}: {str(e)}")

    return last_result


def main():
    parser = argparse.ArgumentParser(description="Spotify Data Pipeline")
    parser.add_argument("--config", default="configs/config.yaml", help="Path to configuration file")
    parser.add_argument("--steps", choices=["all", "single"], default="all",
                        help="Run all steps or single steps")
    parser.add_argument("--formats", nargs="+", default=["parquet", "csv"],
                        help="Output formats for conversion")
    parser.add_argument("--query-file", default="sql/queries.sql", help="SQL query file for Trino queries")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Create timestamp for run-specific directories
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Setup directories
    temp_dir = config['processing']['temp_dir']
    bronze_dir = os.path.join(temp_dir, "bronze", timestamp)
    parsed_dir = os.path.join(temp_dir, "parsed", timestamp)
    cleaned_dir = os.path.join(temp_dir, "cleaned", timestamp)
    silver_dir = os.path.join(temp_dir, "silver", timestamp)
    parquet_dir = os.path.join(temp_dir, "parquet", timestamp)
    query_results_dir = os.path.join(temp_dir, "query_results", timestamp)
    combined_dir = os.path.join(temp_dir, "combined", timestamp)
    analysis_dir = os.path.join(temp_dir, "analysis", timestamp)
    gold_dir = os.path.join(temp_dir, "gold", timestamp)

    # Create necessary directories
    create_directories([
        bronze_dir, parsed_dir, cleaned_dir, silver_dir,
        parquet_dir, query_results_dir, combined_dir, analysis_dir, gold_dir
    ])

    try:
        # STEP 1: Download data from S3 Bronze layer
        logger.info("STEP 1: Downloading data from S3 Bronze layer")
        download_files_from_s3(
            source_bucket=config['s3']['source_bucket'],
            bronze_bucket=config['s3']['bronze_bucket'],
            source_prefix=config['s3']['source_prefix'],
            bronze_prefix=config['s3']['bronze_prefix'],
            local_temp_dir=bronze_dir,
            zip_file_name=config['files']['zip_file'],
            xml_file_name=config['files']['xml_file']
        )
        logger.info("Download process completed")

        # STEP 2: Parse XML files
        logger.info("STEP 2: Parsing XML files")
        xml_path = os.path.join(bronze_dir, config['files']['xml_file'])
        parse_rss_feed(xml_path, parsed_dir)
        logger.info("XML parsing completed")

        # STEP 3: Clean TXT files
        logger.info("STEP 3: Cleaning TXT files")
        clean_data(parsed_dir, cleaned_dir, combined_output=True)
        logger.info("Data cleaning completed")

        # STEP 4: Upload new data to S3 Silver layer
        logger.info("STEP 4: Uploading cleaned data to S3 Silver layer")
        upload_to_silver_layer(
            source_dir=cleaned_dir,
            bucket=config['s3']['silver_bucket'],
            prefix=config['s3']['silver_prefix'],
            file_pattern="*.csv"
        )
        logger.info("Silver layer upload completed")

        # STEP 5: Convert cleaned data to parquet
        logger.info("STEP 5: Converting cleaned data to parquet")
        convert_to_formats(
            input_dir=cleaned_dir,
            output_dir=parquet_dir,
            formats=["parquet"],  # Only convert to parquet in this step
            silver_bucket=config['s3']['silver_bucket'],
            silver_prefix=config['s3']['silver_prefix']
        )
        logger.info("Parquet conversion completed")

        # STEP 6: Setup Trino server
        logger.info("STEP 6: Setting up Trino server")
        trino_config = setup_local_trino_for_parquet(parquet_dir)
        logger.info("Trino server setup completed")

        # STEP 7: Run queries on Trino server
        logger.info("STEP 7: Running queries on Trino server")
        if trino_config is None:
            # Use default Trino configuration if setup didn't return proper config
            logger.warning("Trino configuration is None, using default settings")
            trino_config = {
                'host': 'localhost',
                'port': 8080,
                'user': 'trino',
                'catalog': 'hive',
                'schema': 'default'
            }

        run_trino_queries(
            query_file=args.query_file,
            host=trino_config.get('host', 'localhost'),
            port=trino_config.get('port', 8080),
            user=trino_config.get('user', 'trino'),
            catalog=trino_config.get('catalog', 'hive'),
            schema=trino_config.get('schema', 'default'),
            output_dir=query_results_dir
        )
        logger.info("Trino queries completed")

        # STEP 8: Combine parsed XML(csv) data with new csv
        logger.info("STEP 8: Combining parsed XML data with query results")
        podcast_data = os.path.join(cleaned_dir, "podcasts_combined.csv")
        query_data = os.path.join(query_results_dir, "final_query_result.csv")
        combined_data_path = os.path.join(combined_dir, "combined_data.csv")

        combine_csvs(
            podcast_csv=podcast_data,
            stats_csv=query_data,
            output_csv=combined_data_path
        )
        logger.info("Data combination completed")

        # STEP 9: Run data analysis on the combined data
        logger.info("STEP 9: Running data analysis on combined data")
        analysis_output = os.path.join(analysis_dir, "podcast_analysis.csv")
        analyze_podcasts(
            input_csv=combined_data_path,
            output_dir=analysis_output
        )
        logger.info("Data analysis completed")

        # STEP 10: Upload analyzed data to S3 Gold layer
        logger.info("STEP 10: Uploading analyzed data to S3 Gold layer")
        upload_to_gold_layer(
            source_dir=analysis_dir,
            bucket=config['s3']['gold_bucket'],
            prefix=config['s3']['gold_prefix'],
            file_pattern="*.csv"
        )
        logger.info("Gold layer upload completed")

        logger.info("Pipeline execution completed successfully!")

    except Exception as e:
        logger.error(f"Error in pipeline execution: {str(e)}")
        raise


if __name__ == "__main__":
    main()