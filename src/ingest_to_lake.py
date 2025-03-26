import os
import boto3
import argparse
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")


def get_raw_data_folder(date_prefix=None):
    raw_data_path = "sample_data/raw"
    if not os.path.exists(raw_data_path):
        raise FileNotFoundError(f"Raw data directory {raw_data_path} does not exist")

    folders = [
        f
        for f in os.listdir(raw_data_path)
        if os.path.isdir(os.path.join(raw_data_path, f))
    ]
    if not folders:
        raise FileNotFoundError("No data folders found in raw directory")

    if date_prefix:
        matching_folders = [f for f in folders if f.startswith(date_prefix)]
        if not matching_folders:
            raise FileNotFoundError(
                f"No folders found matching date prefix: {date_prefix}"
            )
        target_folder = sorted(matching_folders)[-1]
    else:
        target_folder = sorted(folders)[-1]

    return os.path.join(raw_data_path, target_folder)


def get_json_files(folder_path, prefix=None):
    files = []
    for file in os.listdir(folder_path):
        if file.endswith(".json"):
            if prefix:
                if file.startswith(prefix):
                    files.append(file)
            else:
                files.append(file)
    return files


def create_spark_session():
    print("Creating Spark session...")
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    return spark


def read_json_to_df(spark, file_path):
    print(f"Reading data from {file_path}...")
    return spark.read.json(file_path)


def write_df_to_parquet(df, file_path):
    print(f"Writing data to {file_path}...")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.write.mode("overwrite").parquet(file_path)


def upload_directory_to_s3(directory_path, bucket_name, s3_key_prefix):
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"The directory {directory_path} does not exist.")

    print(
        f"Uploading directory {directory_path} to S3 bucket {bucket_name} "
        f"with key prefix {s3_key_prefix}..."
    )
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    for root, dirs, files in os.walk(directory_path):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.basename(os.path.dirname(local_path))
            s3_key = f"{s3_key_prefix}/{relative_path}/{os.path.basename(local_path)}"
            print(f"Uploading {local_path} to {s3_key}...")
            s3_client.upload_file(local_path, bucket_name, s3_key)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Process raw data from specific date")
    parser.add_argument("date", nargs="?", type=str, help="Date in format YYYYMMDD")
    parser.add_argument(
        "--prefix", type=str, help="File prefix to process (e.g., 'customers_test')"
    )
    return parser.parse_args()


def main():
    try:
        args = parse_arguments()
        date_prefix = None
        if args.date:
            date_prefix = args.date.lstrip("-")[:8]
            try:
                datetime.strptime(date_prefix, "%Y%m%d")
            except ValueError:
                raise ValueError("Date must be in format YYYYMMDD")

        raw_folder = get_raw_data_folder(date_prefix)
        folder_date = os.path.basename(raw_folder)
        print(f"Processing data from folder: {raw_folder}")

        json_files = get_json_files(raw_folder, args.prefix)
        if not json_files:
            raise FileNotFoundError(
                f"No JSON files found in {raw_folder}"
                + (f" with prefix {args.prefix}" if args.prefix else "")
            )

        spark = create_spark_session()

        for json_file in json_files:
            print(f"\nProcessing {json_file}...")

            base_name = os.path.splitext(json_file)[0]

            input_path = os.path.join(raw_folder, json_file)
            processed_base_path = f"sample_data/processed/{folder_date}"
            processed_path = os.path.join(processed_base_path, base_name)

            df = read_json_to_df(spark, input_path)
            write_df_to_parquet(df, processed_path)

        # Upload all processed data to S3
        s3_prefix = folder_date
        processed_base_path = f"sample_data/processed/{folder_date}"
        upload_directory_to_s3(processed_base_path, AWS_BUCKET_NAME, s3_prefix)

        spark.stop()
        print("\nData processing completed successfully!")
        print(f"Processed data saved to: {processed_base_path}")
        print(f"Data uploaded to S3: s3://{AWS_BUCKET_NAME}/{s3_prefix}/")
        print("Spark session stopped.")

    except Exception as e:
        print(f"Error processing data: {str(e)}")
        if "spark" in locals():
            spark.stop()
        raise


if __name__ == "__main__":
    main()
