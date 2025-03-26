"""
Database Initialization Script for ETL Metadata Framework
--------------------------------------------------------
This script creates necessary database tables for the ETL Metadata-Driven Framework:
1. Controller table: stores pipeline configurations
2. Audit table: tracks pipeline execution history
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


DEFAULT_SQL_FILE = "sql/create_metadata_tables.sql"


def connect_to_database():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logger.info(f"Successfully connected to database {DB_NAME}")
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        sys.exit(1)


def create_tables_from_file(connection, sql_file_path):
    try:
        with open(sql_file_path, "r", encoding="utf-8") as file:
            sql_script = file.read()

        with connection.cursor() as cursor:
            cursor.execute(sql_script)
            connection.commit()

        logger.info(f"Successfully created tables from {sql_file_path}")
        return True
    except Exception as e:
        logger.error(f"Error executing SQL file: {str(e)}")
        connection.rollback()
        return False


def add_controller_entry(
    data_source, source, destination, schema_name="public", load_type="full"
):
    connection = connect_to_database()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM controller WHERE source_table = %s AND destination_table = %s",
                (source, destination),
            )
            existing_row = cursor.fetchone()

            if existing_row:
                cursor.execute(
                    """
                    UPDATE controller
                    SET schema_name = %s, load_type = %s, data_source = %s,
                    updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (schema_name, load_type, data_source, existing_row[0]),
                )
                logger.info(
                    f"Updated configuration: {source} -> {schema_name}.{destination}"
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO controller
                    (data_source, source_table, destination_table, schema_name, load_type)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (data_source, source, destination, schema_name, load_type),
                )
                logger.info(
                    f"Added new configuration: {source} -> {schema_name}.{destination}"
                )

            connection.commit()
            return True

    except Exception as e:
        logger.error(f"Error adding pipeline configuration: {str(e)}")
        connection.rollback()
        return False
    finally:
        connection.close()


def main():
    logger.info("Connecting to database...")
    connection = connect_to_database()

    try:
        if not os.path.exists(DEFAULT_SQL_FILE):
            logger.error(f"SQL file not found: {DEFAULT_SQL_FILE}")
            logger.error("Please create the SQL file or check the path.")
            logger.info("You can use the template in the documentation.")
            sys.exit(1)

        logger.info(f"Creating tables from SQL file: {DEFAULT_SQL_FILE}")
        if not create_tables_from_file(connection, DEFAULT_SQL_FILE):
            logger.error("Failed to create tables from SQL file.")
            sys.exit(1)

        logger.info("Metadata tables created successfully")

    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        connection.rollback()
        sys.exit(1)
    finally:
        connection.close()

    logger.info("Metadata framework initialized successfully!")


if __name__ == "__main__":
    main()
