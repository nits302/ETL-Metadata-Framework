# S3 to PostgreSQL ETL Pipeline

This project loads parquet data from AWS S3 into PostgreSQL tables using Apache Spark.

## Prerequisites

- Python 3.7+
- Apache Spark 3.x
- PostgreSQL database
- AWS S3 access

## Setup

1. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

2. Make sure the .env file is properly configured with your credentials:

   ```
   AWS_ACCESS_KEY_ID=your_aws_access_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret_key
   AWS_BUCKET_NAME=your_bucket_name
   AWS_REGION=your_aws_region

   POSTGRES_HOST=your_postgres_host
   POSTGRES_PORT=your_postgres_port
   POSTGRES_DATABASE=your_database_name
   POSTGRES_USER=your_username
   POSTGRES_PASSWORD=your_password
   ```

3. Download the necessary Spark connector JARs:

   - PostgreSQL JDBC driver (postgresql-42.5.1.jar)
   - Hadoop AWS connector (hadoop-aws-3.3.1.jar)
   - AWS Java SDK bundle (aws-java-sdk-bundle-1.11.901.jar)

   Place these JARs in a directory and set `SPARK_JARS_DIR` environment variable.

## Running the ETL Pipeline

Execute the script using:

```
python spark/ingest_to_postgres.py
```

This will:

1. Read parquet files from the S3 bucket folders (customers and orders)
2. Create tables in PostgreSQL if they don't exist
3. Load the data into those tables

## Troubleshooting

If you encounter issues with S3 connectivity, ensure:

- Your AWS credentials are correct
- The bucket exists and you have access to it
- Network connectivity allows access to S3

For PostgreSQL issues:

- Verify database connection information
- Ensure the user has create/write privileges
