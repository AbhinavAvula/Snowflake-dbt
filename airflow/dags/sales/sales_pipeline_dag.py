
import logging
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from airflow.sdk import DAG, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from urllib.parse import urlparse


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



def register_sales_files(**context):
    ti = context["ti"]
    run_id = context["run_id"]

    discovered_files = ti.xcom_pull(
        task_ids="discover_sales_files"
    )

    if discovered_files is None:
        raise ValueError("File discovery returned no result")

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    # Retrieve the batch associated with this Airflow run
    batch = hook.get_first(
        """
        SELECT BATCH_ID
        FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        WHERE DAG_ID = 'sales_pipeline'
          AND AIRFLOW_RUN_ID = %s
        """,
        parameters=(run_id,),
    )

    if not batch:
        raise ValueError("Batch registration not found")

    batch_id = batch[0]
    new_files = 0
    skipped_files = 0

    for file in discovered_files:
        raw_path = file[0]
        file_size = file[1]
        source_etag = file[2]

        # Convert the S3 path into the canonical registry format
        if raw_path.startswith("s3://"):
            parsed = urlparse(raw_path)
            source_file = parsed.path.lstrip("/")
        else:
            source_file = raw_path.lstrip("/")

            bucket_prefix = "snowflake-dbt-s3-bkt/"
            if source_file.startswith(bucket_prefix):
                source_file = source_file[len(bucket_prefix):]

        # Check whether this source file was previously registered
        existing = hook.get_first(
            """
            SELECT BATCH_ID, SOURCE_ETAG, STATUS
            FROM SALES_DB.CONTROL_SCM.BATCH_FILES
            WHERE SOURCE_FILE = %s
            LIMIT 1
            """,
            parameters=(source_file,),
        )

        if existing:
            existing_batch, existing_etag, status = existing

            # Do not silently process overwritten source files
            if (
                existing_etag is not None
                and source_etag is not None
                and existing_etag != source_etag
            ):
                raise ValueError(
                    f"Source file changed: {source_file}"
                )

            # Previously registered file: leave its batch unchanged
            logging.info(
                "Skipping file %s (batch=%s, status=%s)",
                source_file,
                existing_batch,
                status,
            )
            skipped_files += 1
            continue

        # Register a previously unseen file
        hook.run(
            """
            INSERT INTO SALES_DB.CONTROL_SCM.BATCH_FILES (
                BATCH_ID,
                SOURCE_FILE,
                SOURCE_ETAG,
                FILE_SIZE_BYTES,
                STATUS
            )
            VALUES (%s, %s, %s, %s, 'DISCOVERED')
            """,
            parameters=(
                batch_id,
                source_file,
                source_etag,
                file_size,
            ),
        )

        new_files += 1

    logging.info(
        "Batch %s: %d new files, %d existing files",
        batch_id,
        new_files,
        skipped_files,
    )

    print(f"File discovery registration finished for {batch_id}")


def ingest_registered_sales_files(**context):
    run_id = context["run_id"]

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    # Find the batch for this Airflow execution
    batch = hook.get_first(
        """
        SELECT BATCH_ID
        FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        WHERE DAG_ID = 'sales_pipeline'
          AND AIRFLOW_RUN_ID = %s
        """,
        parameters=(run_id,),
    )

    if not batch:
        raise ValueError("No batch found for this DAG run")

    batch_id = batch[0]

    # Retrieve files assigned to this batch
    files = hook.get_records(
        """
        SELECT SOURCE_FILE
        FROM SALES_DB.CONTROL_SCM.BATCH_FILES
        WHERE BATCH_ID = %s
          AND STATUS = 'DISCOVERED'
        ORDER BY SOURCE_FILE
        """,
        parameters=(batch_id,),
    )

    if not files:
        logging.info("No new files for batch %s", batch_id)
        return

    # Our external stage already points to sales/bronze/
    stage_prefix = "sales/bronze/"
    relative_files = []

    for (source_file,) in files:
        if not source_file.startswith(stage_prefix):
            raise ValueError(
                f"Unexpected source path: {source_file}"
            )

        # COPY FILES paths are relative to the stage URL
        relative_path = source_file[len(stage_prefix):]

        # Escape SQL string literals
        relative_files.append(
            "'" + relative_path.replace("'", "''") + "'"
        )

    # Snowflake supports up to 1,000 files per COPY
    for index in range(0, len(relative_files), 1000):
        file_group = relative_files[index:index + 1000]
        file_list = ", ".join(file_group)

        copy_sql = f"""
            COPY INTO SALES_DB.BRONZE_SCM.RAW_SALES
            FROM @SALES_DB.BRONZE_SCM.SALES_BRONZE_STAGE
            FILES = ({file_list})
            FILE_FORMAT = (
                FORMAT_NAME = 'SALES_DB.BRONZE_SCM.PARQUET_FORMAT'
            )
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                SOURCE_FILE = METADATA$FILENAME,
                SOURCE_FILE_ROW_NUMBER = METADATA$FILE_ROW_NUMBER,
                INGESTED_AT = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
        """

        
        # Execute COPY and capture file-level results
        copy_results = hook.get_records(copy_sql)

        if not copy_results or len(copy_results[0]) < 4:
            raise RuntimeError(
                "COPY did not load any files. Reconciliation required."
            )

        for result in copy_results:
            copied_file = str(result[0])
            copy_status = str(result[1]).upper()
            rows_loaded = int(result[3])

            # Accept only successfully loaded files
            if copy_status != "LOADED":
                raise RuntimeError(
                    f"Unexpected COPY status for {copied_file}: "
                    f"{copy_status}"
                )

            # Match the COPY result to exactly one registered file
            matches = [
                row[0] for row in files
                if (
                    copied_file == row[0]
                    or copied_file.endswith("/" + row[0])
                    or row[0].endswith("/" + copied_file)
                )
            ]

            if len(matches) != 1:
                raise ValueError(
                    f"Cannot uniquely identify loaded file: {copied_file}"
                )

            source_file = matches[0]

            # Tag newly loaded Bronze rows with their batch ID
            hook.run(
                """
                UPDATE SALES_DB.BRONZE_SCM.RAW_SALES
                SET BATCH_ID = %s
                WHERE SOURCE_FILE = %s
                  AND BATCH_ID IS NULL
                """,
                parameters=(batch_id, source_file),
            )

            # Verify that all expected rows were assigned
            tagged_count = hook.get_first(
                """
                SELECT COUNT(*)
                FROM SALES_DB.BRONZE_SCM.RAW_SALES
                WHERE SOURCE_FILE = %s
                  AND BATCH_ID = %s
                """,
                parameters=(source_file, batch_id),
            )[0]

            if tagged_count != rows_loaded:
                raise RuntimeError(
                    f"Batch assignment mismatch for {source_file}: "
                    f"expected {rows_loaded}, found {tagged_count}"
                )

            # Mark this source file as successfully loaded
            hook.run(
                """
                UPDATE SALES_DB.CONTROL_SCM.BATCH_FILES
                SET STATUS = 'LOADED',
                    ROWS_LOADED = %s,
                    LOADED_AT = CURRENT_TIMESTAMP()
                WHERE SOURCE_FILE = %s
                  AND BATCH_ID = %s
                  AND STATUS = 'DISCOVERED'
                """,
                parameters=(rows_loaded, source_file, batch_id),
            )

            logging.info(
                "Loaded %s rows from %s into batch %s",
                rows_loaded,
                source_file,
                batch_id,
            )

    logging.info(
        "Completed Bronze COPY commands for batch %s",
        batch_id,
    )


with DAG(
    dag_id="sales_pipeline",
    description="Sales Bronze to Gold pipeline with data quality and quarantine",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["sales", "snowflake", "dbt", "data-quality"],
) as dag:

    # Register the current Airflow execution as a processing batch
    register_sales_batch = SQLExecuteQueryOperator(
        task_id="register_sales_batch",
        conn_id="snowflake_default",
        sql="""
            MERGE INTO SALES_DB.CONTROL_SCM.PIPELINE_BATCHES AS target

            USING (
                SELECT
                    'B_' || SUBSTR(
                        SHA2(
                            'sales_pipeline|' ||
                            '{{ run_id | replace("'", "''") }}',
                            256
                        ),
                        1, 32
                    ) AS BATCH_ID,
                    'sales_pipeline' AS DAG_ID,
                    '{{ run_id | replace("'", "''") }}' AS AIRFLOW_RUN_ID
            ) AS source

            ON target.BATCH_ID = source.BATCH_ID

            WHEN NOT MATCHED THEN
                INSERT (
                    BATCH_ID,
                    DAG_ID,
                    AIRFLOW_RUN_ID,
                    STATUS
                )
                VALUES (
                    source.BATCH_ID,
                    source.DAG_ID,
                    source.AIRFLOW_RUN_ID,
                    'REGISTERED'
                );
        """,
    )


    # Discover Parquet files available in the S3 external stage
    discover_sales_files = SQLExecuteQueryOperator(
        task_id="discover_sales_files",
        conn_id="snowflake_default",
        sql="""
            LIST @SALES_DB.BRONZE_SCM.SALES_BRONZE_STAGE
            PATTERN = '.*[.]parquet';
        """,
        do_xcom_push=True,
        show_return_value_in_logs=False,
    )

    register_sales_files = PythonOperator(
        task_id="register_sales_files",
        python_callable=register_sales_files,
    )


    
    # Load only the files registered for this batch
    ingest_sales_bronze = PythonOperator(
        task_id="ingest_sales_bronze",
        python_callable=ingest_registered_sales_files,
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
    register_sales_batch >> discover_sales_files
    discover_sales_files >> register_sales_files
    register_sales_files >> ingest_sales_bronze
    ingest_sales_bronze >> validate_sales_dq
    validate_sales_dq >> load_silver_quarantine
    load_silver_quarantine >> check_sales_dq_alerts

    # Email notification and transformations run independently
    check_sales_dq_alerts >> [
        notify_sales_dq,
        run_sales_dbt,
    ]