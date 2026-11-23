
import logging
import smtplib

from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

from airflow.sdk import DAG, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


# Send Gmail notification when invalid records are quarantined
def send_sales_dq_alert(**context):

    # Retrieve the invalid record count from the SQL task
    result = context["ti"].xcom_pull(
        task_ids="check_sales_dq_alerts"
    )

    if not result:
        raise ValueError("Data quality count was not returned")

    invalid_count = int(result[0][0])

    # Skip email when there are no new invalid records
    if invalid_count == 0:
        logging.info("No new invalid records. Email skipped.")
        return

    run_id = context["run_id"]

    # Retrieve Gmail credentials from Airflow Variables
    sender = Variable.get("gmail_sender")
    recipient = Variable.get("gmail_recipient")
    app_password = Variable.get("gmail_app_password")

    # Construct the email
    message = EmailMessage()
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = "Sales Pipeline - Data Quality Alert"

    message.set_content(
        f"""
Sales Data Quality Alert

DAG: sales_pipeline
Run ID: {run_id}

New quarantined records: {invalid_count}

Quarantine Table:
SALES_DB.DQ_SCM.SILVER_QUARANTINE

Please investigate the rejected records.
"""
    )

    # Send email through Gmail SMTP
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
        smtp.login(sender, app_password)
        smtp.send_message(message)

    logging.info("Data quality alert email sent successfully.")


with DAG(
    dag_id="sales_pipeline",
    description="Sales Bronze to Gold pipeline with data quality and quarantine",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["sales", "snowflake", "dbt", "data-quality"],
) as dag:

    # Task 1: Ingest raw sales records from S3 into Bronze
    ingest_sales_bronze = SQLExecuteQueryOperator(
        task_id="ingest_sales_bronze",
        conn_id="snowflake_default",
        sql="""
            COPY INTO SALES_DB.BRONZE_SCM.RAW_SALES
            FROM @SALES_DB.BRONZE_SCM.SALES_BRONZE_STAGE
            FILE_FORMAT = (
                FORMAT_NAME = 'SALES_DB.BRONZE_SCM.PARQUET_FORMAT'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                SOURCE_FILE = METADATA$FILENAME,
                SOURCE_FILE_ROW_NUMBER = METADATA$FILE_ROW_NUMBER,
                INGESTED_AT = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT;
        """,
    )

    # Task 2: Validate Bronze records using dbt
    validate_sales_dq = BashOperator(
        task_id="validate_sales_dq",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/config/dbt "
            "--select sales_dq_validation"
        ),
    )

    # Task 3: Insert invalid records into Silver quarantine
    load_silver_quarantine = SQLExecuteQueryOperator(
        task_id="load_silver_quarantine",
        conn_id="snowflake_default",
        sql=Path(
            "/opt/airflow/dbt/operations/sales/dq/load_silver_quarantine.sql"
        ).read_text(),
    )

    # Task 4: Count newly quarantined records
    check_sales_dq_alerts = SQLExecuteQueryOperator(
        task_id="check_sales_dq_alerts",
        conn_id="snowflake_default",
        sql="""
            SELECT COUNT(*) AS INVALID_RECORD_COUNT
            FROM SALES_DB.DQ_SCM.SILVER_QUARANTINE
            WHERE BATCH_ID = '{{ run_id }}';
        """,
        do_xcom_push=True,
        show_return_value_in_logs=True,
    )

    # Task 5: Send Gmail notification if invalid records exist
    notify_sales_dq = PythonOperator(
        task_id="notify_sales_dq",
        python_callable=send_sales_dq_alert,
    )

    # Task 6: Transform valid records into Silver and Gold
    run_sales_dbt = BashOperator(
        task_id="run_sales_dbt",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/config/dbt "
            "--select clean_sales+"
        ),
    )

    # Execute ingestion, validation and quarantine sequentially
    ingest_sales_bronze >> validate_sales_dq
    validate_sales_dq >> load_silver_quarantine
    load_silver_quarantine >> check_sales_dq_alerts

    # Email notification and transformations run independently
    check_sales_dq_alerts >> [
        notify_sales_dq,
        run_sales_dbt,
    ]