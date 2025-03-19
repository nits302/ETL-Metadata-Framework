import os
import boto3
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# File paths
CUSTOMER_FILE_PATH = "data/raw/customers.json"
ORDER_FILE_PATH = "data/raw/orders.json"
PROCESSED_CUSTOMER_PATH = "data/processed/customers"
PROCESSED_ORDER_PATH = "data/processed/orders"


def create_spark_session():
    print("Creating Spark session...")
    spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
    return spark


def read_json_to_df(spark, file_path):
    print(f"Reading data from {file_path}...")
    return spark.read.option("multiline", "true").json(file_path)


def write_df_to_parquet(df, file_path):
    print(f"Writing data to {file_path}...")
    df.write.mode("overwrite").parquet(file_path)


def upload_directory_to_s3(directory_path, bucket_name, s3_key_prefix):
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"The directory {directory_path} does not exist.")

    print(
        f"Uploading directory {directory_path} to S3 bucket {bucket_name} \
            with key prefix {s3_key_prefix}..."
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
            relative_path = os.path.relpath(local_path, directory_path)
            s3_key = os.path.join(s3_key_prefix, relative_path).replace("\\", "/")
            print(f"Uploading {local_path} to {s3_key}...")
            s3_client.upload_file(local_path, bucket_name, s3_key)


def main():
    spark = create_spark_session()

    # customer data
    customer_df = read_json_to_df(spark, CUSTOMER_FILE_PATH)
    write_df_to_parquet(customer_df, PROCESSED_CUSTOMER_PATH)
    upload_directory_to_s3(PROCESSED_CUSTOMER_PATH, AWS_BUCKET_NAME, "customers")

    # order data
    order_df = read_json_to_df(spark, ORDER_FILE_PATH)
    write_df_to_parquet(order_df, PROCESSED_ORDER_PATH)
    upload_directory_to_s3(PROCESSED_ORDER_PATH, AWS_BUCKET_NAME, "orders")

    spark.stop()
    print("Spark session stopped.")


if __name__ == "__main__":
    main()
