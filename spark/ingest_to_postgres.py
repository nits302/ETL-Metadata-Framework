import os
import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DATABASE")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DB_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


def get_s3_client():
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    return s3_client


def get_db_engine():
    return create_engine(DB_URL)


def list_parquet_files(s3_client, bucket_name, prefix):
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []

    if "Contents" in response:
        for obj in response["Contents"]:
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])

    return files


def ingest_s3_to_postgres(bucket_name, prefix, table_name):
    print(f"Ingesting data from s3://{bucket_name}/{prefix} to table {table_name}")

    # Get S3 client and DB engine
    s3_client = get_s3_client()
    engine = get_db_engine()

    # List parquet files
    parquet_files = list_parquet_files(s3_client, bucket_name, prefix)

    if not parquet_files:
        print(f"No parquet files found in s3://{bucket_name}/{prefix}")
        return

    # Combine all parquet files into one dataframe
    all_dfs = []
    total_rows = 0
    print(f"\nReading parquet files from {prefix}:")
    for file in parquet_files:
        print(f"- Reading file: {file}")
        response = s3_client.get_object(Bucket=bucket_name, Key=file)
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        total_rows += len(df)
        all_dfs.append(df)
        print(f"  - Found {len(df)} rows")

    combined_df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    if combined_df.empty:
        print(f"No data found in parquet files at s3://{bucket_name}/{prefix}")
        return

    print(f"\nTotal rows found in {table_name}: {total_rows}")
    print(f"Sample data for {table_name} (first 5 rows):")
    print(combined_df.head())
    print("\nColumns in dataframe:")
    print(combined_df.columns.tolist())

    # Write to PostgreSQL
    combined_df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    # Verify data in PostgreSQL
    row_count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table_name}", engine).iloc[
        0
    ]["count"]
    print(f"\nVerified {row_count} rows written to PostgreSQL table '{table_name}'")
    print(
        f"Successfully ingested data from s3://{bucket_name}/{prefix} to table {table_name}"
    )
    print("-" * 80 + "\n")


def main():
    CUSTOMERS_PREFIX = "customers/"
    ORDERS_PREFIX = "orders/"

    ingest_s3_to_postgres(AWS_BUCKET_NAME, CUSTOMERS_PREFIX, "customers")

    ingest_s3_to_postgres(AWS_BUCKET_NAME, ORDERS_PREFIX, "orders")

    print("Data ingestion completed successfully!")


if __name__ == "__main__":
    main()
