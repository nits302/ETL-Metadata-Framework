"""
Email Notification Module for ETL Metadata Framework
---------------------------------------------------
This module handles email notifications for ETL pipeline status updates.
It provides functions to send email notifications about pipeline execution results.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

load_dotenv()

EMAIL_HOST = os.getenv("EMAIL_HOST")
EMAIL_PORT = int(os.getenv("EMAIL_PORT"))
EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS").split(",")

# Global storage for pipeline notifications
_pending_notifications = {
    "success": [],
    "failure": [],
    "execution_count": 0,
    "last_updated": datetime.now(),
}


def send_email(subject, body):
    logger.info("=== EMAIL NOTIFICATION ATTEMPT ===")
    logger.info(f"Subject: {subject}")

    if not EMAIL_USER or not EMAIL_PASSWORD or not EMAIL_RECIPIENTS:
        logger.error("Email configuration is missing. No notification will be sent.")
        logger.error(f"User: {EMAIL_USER}")
        logger.error(f"Password set: {'Yes' if EMAIL_PASSWORD else 'No'}")
        logger.error(f"Recipients: {EMAIL_RECIPIENTS}")
        return False

    try:
        logger.info(f"Preparing email to: {', '.join(EMAIL_RECIPIENTS)}")
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)

        msg.attach(MIMEText(body, "plain"))

        logger.info(f"Connecting to {EMAIL_HOST}:{EMAIL_PORT}...")

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            logger.info("Starting TLS...")
            server.starttls()

            logger.info(f"Logging in as {EMAIL_USER}...")
            server.login(EMAIL_USER, EMAIL_PASSWORD)

            logger.info("Sending email...")
            server.sendmail(EMAIL_USER, EMAIL_RECIPIENTS, msg.as_string())

        logger.info("Email notification sent successfully!")
        logger.info(f"Subject: {subject}")
        logger.info("=== END OF EMAIL NOTIFICATION ===")
        return True

    except Exception as e:
        logger.error(f"Failed to send email notification: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        logger.info("=== END OF EMAIL NOTIFICATION ===")
        return False


def notify_pipeline_status(
    pipeline_name,
    status,
    message=None,
    records_processed=None,
    start_time=None,
    end_time=None,
    error_message=None,
):
    """Add pipeline execution status to pending notifications."""

    # Format notification details
    notification = {
        "pipeline": pipeline_name,
        "status": status.upper(),
        "message": message,
        "records": records_processed,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    if error_message:
        notification["error"] = error_message

    # Add to appropriate list
    if status.lower() == "success":
        _pending_notifications["success"].append(notification)
    else:
        _pending_notifications["failure"].append(notification)

    _pending_notifications["execution_count"] += 1
    _pending_notifications["last_updated"] = datetime.now()

    logger.info(f"Added pipeline '{pipeline_name}' ({status}) to pending notifications")

    return True


def send_consolidated_notifications():
    """Send one email with consolidated notifications about all pipeline executions."""

    if _pending_notifications["execution_count"] == 0:
        logger.info("No pending notifications to send")
        return False

    success_count = len(_pending_notifications["success"])
    failure_count = len(_pending_notifications["failure"])
    total_count = success_count + failure_count

    # Create subject line
    if failure_count == 0:
        subject = f"ETL Pipeline Report: All {total_count} pipelines successful"
    else:
        subject = (
            f"ETL Pipeline Report: {failure_count} failures, {success_count} successes"
        )

    # Build message body
    body_parts = [
        "ETL Pipeline Execution Summary",
        "================================",
        f"Execution time: {_pending_notifications['last_updated'].strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total executions: {total_count}",
        f"Successful: {success_count}",
        f"Failed: {failure_count}",
        "",
    ]

    # Add successful pipelines
    if success_count > 0:
        body_parts.append("SUCCESSFUL PIPELINES")
        body_parts.append("===================")
        for notification in _pending_notifications["success"]:
            body_parts.append(
                f"- {notification['pipeline']}: {notification['records'] or 'N/A'} records"
            )
            if notification.get("message"):
                body_parts.append(f"  Message: {notification['message']}")
        body_parts.append("")

    # Add failed pipelines
    if failure_count > 0:
        body_parts.append("FAILED PIPELINES")
        body_parts.append("===============")
        for notification in _pending_notifications["failure"]:
            body_parts.append(f"- {notification['pipeline']}")
            if notification.get("error"):
                body_parts.append(f"  Error: {notification['error']}")
        body_parts.append("")

    body = "\n".join(body_parts)

    # Send the email
    result = send_email(subject, body)

    # Clear pending notifications
    if result:
        _pending_notifications["success"] = []
        _pending_notifications["failure"] = []
        _pending_notifications["execution_count"] = 0

    return result
