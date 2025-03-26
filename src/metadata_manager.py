"""
Metadata Manager Module for ETL Framework
-----------------------------------------
This module handles database interactions for the ETL Metadata-Driven Framework:
1. Retrieving pipeline configurations from the controller table
2. Creating and updating audit records for pipeline executions
3. Managing database connections
"""

import os
import logging
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
from dotenv import load_dotenv

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


def connect_to_database():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return connection
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise


def get_pipeline_config(source_table=None, destination_table=None):
    logger.info("Retrieving pipeline configurations")

    try:
        with connect_to_database() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                query = "SELECT * FROM controller WHERE active = TRUE"
                params = []

                if source_table:
                    query += " AND source_table = %s"
                    params.append(source_table)

                if destination_table:
                    query += " AND destination_table = %s"
                    params.append(destination_table)

                cur.execute(query, params)
                results = [dict(row) for row in cur.fetchall()]

                logger.info(f"Retrieved {len(results)} pipeline configurations")
                return results

    except Exception as e:
        logger.error(f"Error retrieving pipeline configurations: {str(e)}")
        raise


def start_pipeline_audit(pipeline_id):
    logger.info(f"Starting audit record for pipeline ID: {pipeline_id}")

    try:
        with connect_to_database() as conn:
            with conn.cursor() as cur:
                start_time = datetime.now()

                query = """
                    INSERT INTO audit (pipeline_id, status, start_time)
                    VALUES (%s, %s, %s) RETURNING audit_id
                """
                cur.execute(query, (pipeline_id, "running", start_time))

                audit_id = cur.fetchone()[0]
                conn.commit()

                logger.info(f"Created audit record with ID: {audit_id}")
                return audit_id

    except Exception as e:
        logger.error(f"Error creating audit record: {str(e)}")
        raise


def update_pipeline_audit(audit_id, status, records_processed=None, error_message=None):
    logger.info(f"Updating audit record {audit_id} with status: {status}")

    try:
        with connect_to_database() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                end_time = datetime.now()

                if records_processed is not None:
                    try:
                        if hasattr(records_processed, "dtype"):
                            records_processed = int(records_processed)
                    except (TypeError, AttributeError):
                        pass

                cur.execute(
                    """
                    UPDATE audit
                    SET status = %s, records_processed = %s, end_time = %s, error_message = %s
                    WHERE audit_id = %s
                    RETURNING *
                    """,
                    (status, records_processed, end_time, error_message, audit_id),
                )

                result = cur.fetchone()
                if result:
                    audit_record = dict(result)
                    conn.commit()
                    logger.info(f"Updated audit record {audit_id}")
                    return audit_record
                else:
                    logger.warning(f"No audit record found with ID: {audit_id}")
                    conn.commit()
                    return {
                        "audit_id": audit_id,
                        "status": status,
                        "end_time": end_time,
                    }

    except Exception as e:
        logger.error(f"Error updating audit record: {str(e)}")
        return {"audit_id": audit_id, "status": status, "end_time": datetime.now()}


def get_audit_details(audit_id):
    logger.info(f"Retrieving audit details for ID: {audit_id}")

    try:
        with connect_to_database() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(
                    """
                    SELECT a.*, c.source_table, c.destination_table, c.load_type
                    FROM audit a
                    JOIN controller c ON a.pipeline_id = c.id
                    WHERE a.audit_id = %s
                    """,
                    (audit_id,),
                )

                result = cur.fetchone()
                if result:
                    logger.info(f"Found audit record {audit_id}")
                    return dict(result)
                else:
                    logger.warning(f"No audit record found with ID: {audit_id}")
                    return None

    except Exception as e:
        logger.error(f"Error retrieving audit details: {str(e)}")
        raise
