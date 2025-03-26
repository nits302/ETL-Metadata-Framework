"""
ETL Data Ingestion and Transformation Module
-------------------------------------------
This script handles the complete ETL process:
1. Reading data from S3 (Parquet files)
2. Loading it into bronze PostgreSQL tables
3. Using dbt for transformations from bronze to silver and gold
4. Tracking execution in the metadata framework
"""

import os
import logging
import pandas as pd
import boto3
import io
import time
import traceback
import subprocess
import json
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.metadata_manager import (
    get_pipeline_config,
    start_pipeline_audit,
    update_pipeline_audit,
)
from src.notification import notify_pipeline_status, send_consolidated_notifications
import argparse
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Path to dbt project
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", os.path.join(os.getcwd(), "dbt_project"))


def get_s3_client():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION,
        )
        logger.info("S3 client created successfully")
        return s3_client
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        raise


def get_db_engine():
    try:
        logger.info(f"Connecting to PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        engine = create_engine(DB_URL)
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {str(e)}")
        raise


def list_parquet_files(s3_client, bucket_name, prefix):
    try:
        logger.info(f"Listing files from s3://{bucket_name}/{prefix}...")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = []

        if "Contents" in response:
            for obj in response["Contents"]:
                if obj["Key"].endswith(".parquet"):
                    files.append(obj["Key"])

        logger.info(f"Found {len(files)} Parquet files")
        return files
    except Exception as e:
        logger.error(f"Error listing files from S3: {str(e)}")
        return []


def ingest_s3_to_postgres(pipeline_config, date_prefix=None):
    """
    Load data from S3 into PostgreSQL public schema
    """
    data_source = pipeline_config["data_source"]  # Tên nguồn dữ liệu trên S3
    source_table = pipeline_config["source_table"]  # Tên bảng trong PostgreSQL
    load_type = pipeline_config["load_type"]

    logger.info(f"Starting data import: '{data_source}' -> 'public.{source_table}'")
    logger.info(f"Load type: {load_type}")

    if date_prefix:
        prefix = f"{date_prefix}/{data_source}/"
    else:
        prefix = f"{data_source}/"

    logger.info(f"Source: s3://{AWS_BUCKET_NAME}/{prefix}")
    logger.info(f"Destination: {DB_NAME}.public.{source_table}")

    start_time = time.time()

    try:
        s3_client = get_s3_client()
        engine = get_db_engine()

        parquet_files = list_parquet_files(s3_client, AWS_BUCKET_NAME, prefix)

        if not parquet_files:
            error_msg = f"No Parquet files found in s3://{AWS_BUCKET_NAME}/{prefix}"
            logger.error(error_msg)
            return False, 0, error_msg

        all_dfs = []
        total_rows = 0
        logger.info(f"Reading data from {len(parquet_files)} files:")

        for index, file in enumerate(parquet_files):
            logger.info(f"  [{index+1}/{len(parquet_files)}] Reading: {file}")
            try:
                response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=file)
                df = pd.read_parquet(io.BytesIO(response["Body"].read()))
                rows = len(df)
                total_rows += rows
                all_dfs.append(df)
                logger.info(f"Successfully read: {rows} rows")
            except Exception as e:
                logger.error(f"Error reading file: {str(e)}")
                raise

        if all_dfs:
            logger.info(f"Combining {len(all_dfs)} DataFrames...")
            combined_df = pd.concat(all_dfs, ignore_index=True)
        else:
            combined_df = pd.DataFrame()

        if combined_df.empty:
            error_msg = (
                "No data in Parquet files at " f"s3://{AWS_BUCKET_NAME}/{prefix}"
            )
            logger.error(error_msg)
            return False, 0, error_msg

        logger.info(f"Successfully read: {total_rows} rows")
        logger.info("DataFrame information:")
        logger.info(f"  - Rows: {len(combined_df)}")
        logger.info(f"  - Columns: {len(combined_df.columns)}")
        logger.info(f"  - Column names: {', '.join(combined_df.columns.tolist())}")

        logger.info(f"Writing data to PostgreSQL table 'public.{source_table}'...")

        if_exists = "append"
        if load_type.lower() == "full":
            if_exists = "replace"
            logger.info("Full load: existing data will be replaced")
        else:
            logger.info("Incremental load: data will be appended to existing table")

        # Import into public schema
        combined_df.to_sql(
            name=source_table,  # Sử dụng source_table cho tên bảng PostgreSQL
            con=engine,
            schema="public",
            if_exists=if_exists,
            index=False,
            chunksize=1000,
            method="multi",
        )

        verify_query = f"SELECT COUNT(*) as count FROM public.{source_table}"
        row_count = pd.read_sql(verify_query, engine).iloc[0]["count"]

        elapsed_time = time.time() - start_time

        logger.info(f"Successfully wrote {row_count} rows to 'public.{source_table}'")
        logger.info(f"Execution time: {elapsed_time:.2f} seconds")

        return True, row_count, None

    except Exception as e:
        error_message = (
            "Error importing data from "
            f"'{data_source}' to 'public.{source_table}': {str(e)}"
        )
        logger.error(error_message)
        logger.error(traceback.format_exc())
        return False, 0, error_message


def run_dbt_command(
    command, target=None, select=None, vars_dict=None, full_refresh=False
):
    """
    Run a dbt command with specified options

    Parameters:
    -----------
    command : str
        dbt command to run (e.g., 'run', 'test', 'build')
    target : str, optional
        dbt target profile to use
    select : str, optional
        dbt selector expression
    vars_dict : dict, optional
        Variables to pass to dbt
    full_refresh : bool, optional
        Whether to add --full-refresh flag
    """
    try:
        dbt_cmd = ["dbt", command, "--project-dir", DBT_PROJECT_DIR]

        if target:
            dbt_cmd.extend(["--target", target])

        if select:
            dbt_cmd.extend(["--select", select])

        if full_refresh:
            dbt_cmd.append("--full-refresh")

        if vars_dict:
            # Convert dict to JSON string for dbt --vars
            vars_str = json.dumps(vars_dict)
            dbt_cmd.extend(["--vars", vars_str])

        # Log the command
        logger.info(f"Running dbt command: {' '.join(dbt_cmd)}")

        # Run the command and capture output
        process = subprocess.Popen(
            dbt_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        # Stream output in real-time
        stdout, stderr = process.communicate()

        if stdout:
            for line in stdout.split("\n"):
                if line.strip():
                    logger.info(f"dbt: {line}")

        if stderr:
            for line in stderr.split("\n"):
                if line.strip():
                    logger.error(f"dbt error: {line}")

        if process.returncode != 0:
            logger.error(f"dbt command failed with exit code {process.returncode}")
            return False, f"dbt command failed with exit code {process.returncode}"

        return True, None

    except Exception as e:
        error_msg = f"Error running dbt command: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        return False, error_msg


def get_dbt_run_results():
    """Get run results from dbt run artifacts"""
    try:
        # Default dbt target path
        target_path = os.path.join(DBT_PROJECT_DIR, "target")
        run_results_path = os.path.join(target_path, "run_results.json")

        if not os.path.exists(run_results_path):
            logger.warning(f"No run results found at {run_results_path}")
            return None

        with open(run_results_path, "r") as f:
            results = json.load(f)

        # Count successful models
        success_count = 0
        error_count = 0
        total_rows = 0

        for result in results.get("results", []):
            if result.get("status") == "success":
                success_count += 1
                # Extract row count if available (dbt adds this to adapter_response)
                adapter_response = result.get("adapter_response", {})
                rows = adapter_response.get("rows_affected", 0)
                total_rows += rows
            else:
                error_count += 1

        logger.info(
            f"dbt results: {success_count} successful models, {error_count} failed"
        )
        if error_count > 0:
            return False, 0, "Some dbt models failed"

        return True, total_rows, None

    except Exception as e:
        error_msg = f"Error reading dbt run results: {str(e)}"
        logger.error(error_msg)
        return False, 0, error_msg


def transform_with_dbt(pipeline_config):
    """
    Transform data using dbt
    """
    table_name = pipeline_config["destination_table"]
    load_type = pipeline_config["load_type"].lower()
    schema_name = pipeline_config.get("schema_name", "public")

    # Determine which models to run based on target schema
    if schema_name == "bronze":
        layer = "bronze"
    elif schema_name == "silver":
        layer = "silver"
    elif schema_name == "gold":
        layer = "gold"
    else:
        layer = schema_name

    # DBT models typically follow the pattern: layer.model_name
    select_pattern = f"{layer}.{table_name}"

    # Set up variables to pass to dbt
    vars_dict = {
        "source_table": pipeline_config["source_table"],
        "date": datetime.now().strftime("%Y-%m-%d"),
    }

    # Determine if we need full refresh
    full_refresh = load_type == "full"

    # Execute dbt
    success, error_msg = run_dbt_command(
        command="run",
        select=select_pattern,
        vars_dict=vars_dict,
        full_refresh=full_refresh,
    )

    if not success:
        return False, 0, error_msg

    # Get results from dbt run
    return get_dbt_run_results()


def process_pipeline(pipeline_config, date_prefix=None, skip_transform=False):
    """
    Process the ETL pipeline with the following strategy:
    1. For public schema: Extract data from S3 to PostgreSQL
    2. For silver and gold schemas: Use dbt models to transform data from PostgreSQL
    3. Send notifications
    """
    pipeline_id = pipeline_config["pipeline_id"]
    data_source = pipeline_config["data_source"]  # Nguồn dữ liệu S3
    source_table = pipeline_config["source_table"]  # Bảng nguồn trong PostgreSQL
    destination_table = pipeline_config["destination_table"]
    schema_name = pipeline_config.get("schema_name", "public")
    load_type = pipeline_config["load_type"]

    logger.info(f"Starting pipeline {pipeline_id}")
    logger.info(f"S3 Source: {data_source}")
    logger.info(f"PostgreSQL Table: {source_table}")
    logger.info(f"Destination: {schema_name}.{destination_table}")
    logger.info(f"Load type: {load_type}")

    audit_id = start_pipeline_audit(pipeline_id)

    try:
        # Step 1: For public schema, extract from S3 to PostgreSQL
        if schema_name.lower() == "public":
            logger.info("Processing public schema: Extracting data from S3")
            # Giữ nguyên source_table và destination_table
            success, row_count, error_msg = ingest_s3_to_postgres(
                pipeline_config, date_prefix
            )

            if not success:
                update_pipeline_audit(audit_id, "failed", 0, error_msg)
                notify_pipeline_status(pipeline_id, "failure", error_message=error_msg)
                return False, error_msg
        else:
            # For non-public schemas, we'll skip S3 extraction and only do transformations
            logger.info(
                f"Processing {schema_name} layer: Using existing data in PostgreSQL"
            )
            row_count = 0  # Will be updated after transformation

        # Step 2: Transform using dbt models if requested
        if not skip_transform:
            logger.info(f"Using dbt to transform data to {schema_name} layer")
            success, transform_row_count, error_msg = transform_with_dbt(
                pipeline_config
            )

            if not success:
                update_pipeline_audit(audit_id, "failed", row_count, error_msg)
                notify_pipeline_status(pipeline_id, "failure", error_message=error_msg)
                return False, error_msg

            # Update row count with transformed data
            row_count = transform_row_count

        # Update audit status to completed
        update_pipeline_audit(
            audit_id,
            "completed",
            row_count,
            None,
        )

        # Step 3: Send notification
        notify_pipeline_status(
            pipeline_id,
            "success",
            message="Pipeline completed successfully",
            records_processed=row_count,
        )

        return True, None

    except Exception as e:
        error_msg = f"Pipeline failed: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())

        update_pipeline_audit(
            audit_id,
            "failed",
            0,
            error_msg,
        )

        notify_pipeline_status(pipeline_id, "failure", error_message=error_msg)

        return False, error_msg


def create_required_schemas():
    """
    Create all required schemas for ETL process
    """
    try:
        engine = get_db_engine()
        schemas = ["bronze", "silver", "gold"]

        with engine.connect() as conn:
            for schema in schemas:
                logger.info(f"Creating schema {schema} if not exists")
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

        logger.info("All required schemas created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating schemas: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def main():
    """
    Main function to run the ETL pipeline.
    The pipeline follows two phases:
    Phase 1: Load all tables from S3 to PostgreSQL public schema
    Phase 2: Run dbt transformations once for all tables
    """
    try:
        parser = argparse.ArgumentParser(
            description="Run ETL pipeline for specific date"
        )
        parser.add_argument("--date", type=str, help="Date in format YYYYMMDD")
        parser.add_argument(
            "--pipeline-id", type=str, help="Run specific pipeline by ID"
        )
        parser.add_argument(
            "--load-type",
            type=str,
            choices=["full", "incremental"],
            help="Override load type (full or incremental)",
        )
        parser.add_argument(
            "--skip-load",
            action="store_true",
            help="Skip loading data from S3 to PostgreSQL, only run dbt transformations",
        )
        args = parser.parse_args()

        date_prefix = None
        if args.date:
            try:
                date_prefix = args.date.lstrip("-")[:8]
                datetime.strptime(date_prefix, "%Y%m%d")
            except ValueError:
                logger.error("Date must be in format YYYYMMDD")
                return False

        pipeline_configs = get_pipeline_config()
        if not pipeline_configs:
            logger.error("No pipeline configuration found")
            return False

        # Filter specific pipeline if requested
        if args.pipeline_id:
            logger.info(f"Running only pipeline with ID: {args.pipeline_id}")
            pipeline_configs = [
                p for p in pipeline_configs if p["id"] == args.pipeline_id
            ]
            if not pipeline_configs:
                logger.error(f"No pipeline found with ID: {args.pipeline_id}")
                return False

        success_count = 0
        failure_count = 0

        # Phase 1: Load all tables from S3 to PostgreSQL (skip if --skip-load is set)
        if not args.skip_load:
            logger.info("Phase 1: Loading tables from S3 to PostgreSQL")
            for pipeline_config in pipeline_configs:
                pipeline_config["pipeline_id"] = pipeline_config["id"]

                # Override load type if specified
                if args.load_type:
                    original_load_type = pipeline_config["load_type"]
                    pipeline_config["load_type"] = args.load_type
                    logger.info(
                        f"Overriding load type for pipeline {pipeline_config['id']}: "
                        f"{original_load_type} -> {args.load_type}"
                    )

                # Force all tables to be loaded to public schema
                pipeline_config["schema_name"] = "public"
                success, error_msg = process_pipeline(
                    pipeline_config, date_prefix, skip_transform=True
                )
                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    logger.error(
                        f"Pipeline {pipeline_config['id']} failed: {error_msg}"
                    )
        else:
            logger.info("Skipping Phase 1: Loading data from S3 to PostgreSQL")

        # Phase 2: Run dbt transformations
        logger.info("Phase 2: Running dbt transformations")
        success, error_msg = run_dbt_command(
            command="run",
            full_refresh=args.load_type == "full" if args.load_type else False,
        )

        if not success:
            logger.error(f"dbt transformation failed: {error_msg}")
            failure_count += 1
        else:
            success_count += 1

        # Send consolidated email notification for all pipelines
        send_consolidated_notifications()

        total_pipelines = success_count + failure_count
        logger.info(
            f"Pipeline processing completed: {success_count}/{total_pipelines} successful"
        )
        return True

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        logger.error(traceback.format_exc())
        # Send notifications even if there was an exception
        send_consolidated_notifications()
        return False


if __name__ == "__main__":
    main()
