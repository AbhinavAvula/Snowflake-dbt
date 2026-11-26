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
    # Identify the batch associated with this Airflow run
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

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
        raise ValueError("Batch not found for Gmail notification")

    batch_id = batch[0]


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
            Airflow Run ID: {run_id}
            Batch ID: {batch_id}

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

            # An unfinished claim from another batch needs explicit recovery.
            if status == "DISCOVERED" and existing_batch != batch_id:
                raise RuntimeError(
                    f"File {source_file} remains assigned to unfinished batch "
                    f"{existing_batch}. Resume or reconcile that batch first."
                )
            if status not in ("DISCOVERED", "LOADED", "SKIPPED"):
                raise RuntimeError(
                    f"File {source_file} has unresolved status {status} "
                    f"in batch {existing_batch}"
                )

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
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Identify the batch for this Airflow run
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
        raise ValueError("Batch not found")

    batch_id = batch[0]

    # First recover any files loaded by an interrupted attempt
    recover_sales_ingestion(hook, batch_id)

    # Find files still awaiting ingestion
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
        logging.info("Batch %s has no pending files", batch_id)
        return

    stage_prefix = "sales/bronze/"

    # Process one file at a time for precise reconciliation
    for (source_file,) in files:

        if not source_file.startswith(stage_prefix):
            raise ValueError(
                f"Unexpected source path: {source_file}"
            )

        relative_file = source_file[len(stage_prefix):]

        if not relative_file:
            raise ValueError("Empty source filename")

        # Escape the filename for the Snowflake SQL literal
        escaped_file = relative_file.replace("'", "''")

        copy_sql = f"""
            COPY INTO SALES_DB.BRONZE_SCM.RAW_SALES
            FROM @SALES_DB.BRONZE_SCM.SALES_BRONZE_STAGE
            FILES = ('{escaped_file}')
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

        # Execute COPY and inspect the returned status
        copy_results = hook.get_records(copy_sql)

        if not copy_results:
            raise RuntimeError(
                f"No COPY result returned for {source_file}"
            )

        # A zero-files response is NOT proof of success
        if len(copy_results[0]) < 4:
            raise RuntimeError(
                f"COPY processed no files for {source_file}. "
                "Check Snowflake load history before retrying."
            )

        if len(copy_results) != 1:
            raise RuntimeError(
                f"Unexpected COPY result count for {source_file}"
            )

        result = copy_results[0]
        copied_file = str(result[0])
        status = str(result[1]).upper()
        rows_loaded = int(result[3] or 0)

        if status != "LOADED":
            raise RuntimeError(
                f"COPY status for {source_file}: {status}"
            )

        # Ensure the returned file matches the registered file
        if not (
            copied_file == source_file
            or copied_file.endswith("/" + source_file)
        ):
            raise RuntimeError(
                f"COPY returned an unexpected file: {copied_file}"
            )

        # Empty files require separate handling
        if rows_loaded == 0:
            raise RuntimeError(
                f"File loaded zero rows: {source_file}"
            )

        # Assign only previously unassigned Bronze records
        hook.run(
            """
            UPDATE SALES_DB.BRONZE_SCM.RAW_SALES
            SET BATCH_ID = %s
            WHERE SOURCE_FILE = %s
              AND BATCH_ID IS NULL
            """,
            parameters=(batch_id, source_file),
        )

        # Verify record ownership and count
        counts = hook.get_first(
            """
            SELECT
                COUNT(*),
                COUNT_IF(BATCH_ID = %s)
            FROM SALES_DB.BRONZE_SCM.RAW_SALES
            WHERE SOURCE_FILE = %s
            """,
            parameters=(batch_id, source_file),
        )

        total_rows = int(counts[0] or 0)
        assigned_rows = int(counts[1] or 0)

        if total_rows != rows_loaded or assigned_rows != rows_loaded:
            raise RuntimeError(
                f"Batch assignment mismatch for {source_file}: "
                f"COPY={rows_loaded}, "
                f"Bronze={total_rows}, "
                f"Assigned={assigned_rows}"
            )

        # Mark the file successfully ingested
        hook.run(
            """
            UPDATE SALES_DB.CONTROL_SCM.BATCH_FILES
            SET STATUS = 'LOADED',
                ROWS_LOADED = %s,
                LOADED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s
              AND SOURCE_FILE = %s
              AND STATUS = 'DISCOVERED'
            """,
            parameters=(rows_loaded, batch_id, source_file),
        )

        logging.info(
            "Batch %s: loaded %s rows from %s",
            batch_id,
            rows_loaded,
            source_file,
        )




def recover_sales_ingestion(hook, batch_id):

    # Find files still awaiting successful ingestion
    pending_files = hook.get_records(
        """
        SELECT SOURCE_FILE
        FROM SALES_DB.CONTROL_SCM.BATCH_FILES
        WHERE BATCH_ID = %s
          AND STATUS = 'DISCOVERED'
        """,
        parameters=(batch_id,),
    )

    for (source_file,) in pending_files:

        # Check whether Bronze already contains records from this file
        counts = hook.get_first(
            """
            SELECT
                COUNT(*),
                COUNT_IF(BATCH_ID IS NULL),
                COUNT_IF(BATCH_ID = %s),
                COUNT_IF(BATCH_ID IS NOT NULL AND BATCH_ID <> %s)
            FROM SALES_DB.BRONZE_SCM.RAW_SALES
            WHERE SOURCE_FILE = %s
            """,
            parameters=(batch_id, batch_id, source_file),
        )

        total, unassigned, assigned, other_batch = (
            int(value or 0) for value in counts
        )

        # No existing records: leave the file for normal COPY
        if total == 0:
            continue

        # Never claim records belonging to another batch
        if other_batch > 0:
            raise RuntimeError(
                f"File already belongs to another batch: {source_file}"
            )

        # Confirm the file was successfully loaded by Snowflake
        history = hook.get_records(
            """
            SELECT FILE_NAME, STATUS, ROW_COUNT
            FROM TABLE(
                SALES_DB.INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => 'SALES_DB.BRONZE_SCM.RAW_SALES',
                    START_TIME => DATEADD(
                        'DAY', -14, CURRENT_TIMESTAMP()
                    )
                )
            )
            WHERE STATUS = 'Loaded'
            ORDER BY LAST_LOAD_TIME DESC
            """
        )

        # Match the exact canonical S3 object path
        matching_loads = [
            row for row in history
            if (
                str(row[0]).removeprefix(
                    "s3://snowflake-dbt-s3-bkt/"
                ) == source_file
            )
        ]

        if not matching_loads:
            raise RuntimeError(
                f"No successful COPY_HISTORY entry for {source_file}. "
                "Manual reconciliation required."
            )

        # Most recent successful loading of this source path.
        # This still requires immutable paths and a recent history record.
        expected_rows = int(matching_loads[0][2] or 0)

        if total != expected_rows:
            raise RuntimeError(
                f"Row count mismatch for {source_file}: "
                f"Bronze={total}, COPY_HISTORY={expected_rows}"
            )

        # Assign untagged rows to their registered batch
        if unassigned > 0:
            hook.run(
                """
                UPDATE SALES_DB.BRONZE_SCM.RAW_SALES
                SET BATCH_ID = %s
                WHERE SOURCE_FILE = %s
                  AND BATCH_ID IS NULL
                """,
                parameters=(batch_id, source_file),
            )

        # Confirm every row now belongs to the expected batch
        verified = hook.get_first(
            """
            SELECT COUNT(*)
            FROM SALES_DB.BRONZE_SCM.RAW_SALES
            WHERE SOURCE_FILE = %s
              AND BATCH_ID = %s
            """,
            parameters=(source_file, batch_id),
        )[0]

        if int(verified) != expected_rows:
            raise RuntimeError(
                f"Recovery verification failed: {source_file}"
            )

        # Mark ingestion complete in the file registry
        hook.run(
            """
            UPDATE SALES_DB.CONTROL_SCM.BATCH_FILES
            SET STATUS = 'LOADED',
                ROWS_LOADED = %s,
                LOADED_AT = CURRENT_TIMESTAMP()
            WHERE BATCH_ID = %s
              AND SOURCE_FILE = %s
              AND STATUS = 'DISCOVERED'
            """,
            parameters=(expected_rows, batch_id, source_file),
        )

        logging.info(
            "Recovered %s rows from %s for batch %s",
            expected_rows,
            source_file,
            batch_id,
        )



def update_sales_batch_ingestion(**context):
    run_id = context["run_id"]
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Identify the current batch
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
        raise ValueError("Batch not found")

    batch_id = batch[0]

    # Calculate file and ingestion counts
    counts = hook.get_first(
        """
        SELECT
            COUNT(*) AS FILE_COUNT,
            COUNT_IF(STATUS = 'LOADED') AS LOADED_FILES,
            COALESCE(SUM(ROWS_LOADED), 0) AS TOTAL_ROWS
        FROM SALES_DB.CONTROL_SCM.BATCH_FILES
        WHERE BATCH_ID = %s
        """,
        parameters=(batch_id,),
    )

    file_count = int(counts[0])
    loaded_files = int(counts[1] or 0)
    total_rows = int(counts[2])

    # A batch with zero files needs an explicit no-data policy.
    if file_count == 0:
        raise RuntimeError(
            f"Batch {batch_id} discovered no new files. "
            "No-data completion or skip handling is not implemented yet."
        )

    # Do not advance if ingestion is incomplete
    if file_count != loaded_files:
        raise RuntimeError(
            f"Batch {batch_id}: "
            f"{loaded_files}/{file_count} files loaded"
        )

    # Record ingestion progress in the control table
    hook.run(
        """
        UPDATE SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        SET STATUS = 'RUNNING',
            DISCOVERED_FILE_COUNT = %s,
            INGESTED_ROW_COUNT = %s
        WHERE BATCH_ID = %s
        """,
        parameters=(file_count, total_rows, batch_id),
    )

    logging.info(
        "Batch %s: %d files, %d rows ingested",
        batch_id, file_count, total_rows
    )


def update_sales_dq_counts(**context):
    run_id = context["run_id"]

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    # Find the batch associated with this Airflow execution
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
        raise ValueError("Batch not found")

    batch_id = batch[0]

    # Count valid and invalid records in this batch
    counts = hook.get_first(
        """
        SELECT
            COUNT_IF(DQ_STATUS = 'VALID'),
            COUNT_IF(DQ_STATUS = 'INVALID')
        FROM SALES_DB.SILVER_SCM.SALES_DQ_VALIDATION
        WHERE BATCH_ID = %s
        """,
        parameters=(batch_id,),
    )

    valid_count = int(counts[0] or 0)
    invalid_count = int(counts[1] or 0)

    # Store the validation counts in the control table
    hook.run(
        """
        UPDATE SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        SET VALID_ROW_COUNT = %s,
            INVALID_ROW_COUNT = %s
        WHERE BATCH_ID = %s
        """,
        parameters=(valid_count, invalid_count, batch_id),
    )

    logging.info(
        "Batch %s: %d valid, %d invalid records",
        batch_id,
        valid_count,
        invalid_count,
    )


def complete_sales_batch(**context):
    run_id = context["run_id"]

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_default"
    )

    # Identify the current processing batch
    batch = hook.get_first(
        """
        SELECT
            BATCH_ID,
            INGESTED_ROW_COUNT,
            VALID_ROW_COUNT,
            INVALID_ROW_COUNT
        FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        WHERE DAG_ID = 'sales_pipeline'
          AND AIRFLOW_RUN_ID = %s
        """,
        parameters=(run_id,),
    )

    if not batch:
        raise ValueError("Batch not found")

    batch_id = batch[0]
    ingested_count = int(batch[1] or 0)
    valid_count = int(batch[2] or 0)
    invalid_count = int(batch[3] or 0)

    # Count records successfully processed in Silver and Gold
    counts = hook.get_first(
        """
        SELECT
            (SELECT COUNT(*)
             FROM SALES_DB.SILVER_SCM.CLEAN_SALES
             WHERE BATCH_ID = %s),

            (SELECT COUNT(*)
             FROM SALES_DB.GOLD_SCM.FACT_SALES
             WHERE BATCH_ID = %s)
        """,
        parameters=(batch_id, batch_id),
    )

    silver_count = int(counts[0])
    gold_count = int(counts[1])

    # Reconcile all processing layers
    if ingested_count != valid_count + invalid_count:
        raise RuntimeError(
            f"Batch {batch_id}: Bronze/DQ count mismatch"
        )

    if silver_count != valid_count:
        raise RuntimeError(
            f"Batch {batch_id}: Silver count mismatch"
        )

    if gold_count != valid_count:
        raise RuntimeError(
            f"Batch {batch_id}: Gold count mismatch"
        )

    # Mark the batch as completed only after reconciliation
    hook.run(
        """
        UPDATE SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        SET STATUS = 'COMPLETED',
            GOLD_ROW_COUNT = %s,
            COMPLETED_AT = CURRENT_TIMESTAMP(),
            ERROR_MESSAGE = NULL
        WHERE BATCH_ID = %s
        """,
        parameters=(gold_count, batch_id),
    )

    logging.info(
        "Batch %s completed successfully: %d Gold records",
        batch_id,
        gold_count,
    )


def mark_sales_batch_failed(context):
    run_id = context["run_id"]
    task_id = context["task_instance"].task_id
    error = context.get("exception")

    # Record the task that caused the failure
    error_message = f"{task_id}: {str(error) if error else 'Task failed'}"

    # Keep the error message within a manageable length
    error_message = error_message[:4000]

    try:
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_default"
        )

        # Find the registered batch for this Airflow run
        batch = hook.get_first(
            """
            SELECT BATCH_ID
            FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
            WHERE DAG_ID = 'sales_pipeline'
              AND AIRFLOW_RUN_ID = %s
            """,
            parameters=(run_id,),
        )

        # Registration itself may have failed
        if not batch:
            logging.error(
                "No batch found for failed run %s", run_id
            )
            return

        batch_id = batch[0]

        # Mark the batch failed without overwriting a completed batch
        hook.run(
            """
            UPDATE SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
            SET STATUS = 'FAILED',
                ERROR_MESSAGE = %s
            WHERE BATCH_ID = %s
              AND STATUS <> 'COMPLETED'
            """,
            parameters=(error_message, batch_id),
        )

        logging.error(
            "Batch %s marked FAILED. Task: %s",
            batch_id,
            task_id,
        )

    except Exception:
        # Preserve the original task failure
        logging.exception(
            "Unable to update failure status for run %s",
            run_id,
        )


with DAG(
    dag_id="sales_pipeline",
    description="Sales Bronze to Gold pipeline with data quality and quarantine",
    schedule=None,
    start_date=datetime(2026, 11, 1),
    max_active_runs=1,
    catchup=False,
    tags=["sales", "snowflake", "dbt", "data-quality"],
) as dag:

    # Register the current Airflow execution as a processing batch
    
    # Register a new batch or resume an existing failed batch
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

            -- Resume an existing failed batch
            WHEN MATCHED AND target.STATUS = 'FAILED' THEN
                UPDATE SET
                    STATUS = 'REGISTERED',
                    ERROR_MESSAGE = NULL,
                    COMPLETED_AT = NULL

            -- Register a new batch
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

  
    # Validate only the records belonging to this Airflow run
    validate_sales_dq = BashOperator(
        task_id="validate_sales_dq",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/config/dbt "
            "--select sales_dq_validation "
            '--vars \'{"airflow_run_id": {{ run_id | tojson }}}\''
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

    
    # Count quarantined records belonging to the current batch
    check_sales_dq_alerts = SQLExecuteQueryOperator(
        task_id="check_sales_dq_alerts",
        conn_id="snowflake_default",
        sql="""
            SELECT COUNT(*) AS INVALID_RECORD_COUNT
            FROM SALES_DB.DQ_SCM.SILVER_QUARANTINE
            WHERE BATCH_ID = (
                SELECT BATCH_ID
                FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
                WHERE DAG_ID = 'sales_pipeline'
                  AND AIRFLOW_RUN_ID =
                      '{{ run_id | replace("'", "''") }}'
            );
        """,
        do_xcom_push=True,
        show_return_value_in_logs=True,
    )


    # Task 5: Send Gmail notification if invalid records exist
    notify_sales_dq = PythonOperator(
        task_id="notify_sales_dq",
        python_callable=send_sales_dq_alert,
    )

 
    # Process only the current batch in Silver and Gold
    run_sales_dbt = BashOperator(
        task_id="run_sales_dbt",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/config/dbt "
            "--select clean_sales+ "
            """--vars '{"airflow_run_id": {{ run_id | tojson }}}'"""
        ),
    )


    # Reconcile Bronze ingestion and update batch progress
    update_batch_ingestion = PythonOperator(
        task_id="update_batch_ingestion",
        python_callable=update_sales_batch_ingestion,
    )


    # Record Data Quality results for the current batch
    update_batch_dq_counts = PythonOperator(
        task_id="update_batch_dq_counts",
        python_callable=update_sales_dq_counts,
    )


    # Reconcile all layers and complete the processing batch
    complete_batch = PythonOperator(
        task_id="complete_batch",
        python_callable=complete_sales_batch,
    )


    # Monitor failures in core data-processing tasks
    core_pipeline_tasks = [
        register_sales_batch,
        discover_sales_files,
        register_sales_files,
        ingest_sales_bronze,
        update_batch_ingestion,
        validate_sales_dq,
        update_batch_dq_counts,
        load_silver_quarantine,
        check_sales_dq_alerts,
        run_sales_dbt,
        complete_batch,
    ]

    for task in core_pipeline_tasks:
        task.on_failure_callback = mark_sales_batch_failed



    # Execute ingestion, validation and quarantine sequentially
    register_sales_batch >> discover_sales_files
    discover_sales_files >> register_sales_files
    register_sales_files >> ingest_sales_bronze
    ingest_sales_bronze >> update_batch_ingestion
    update_batch_ingestion >> validate_sales_dq
    validate_sales_dq >> update_batch_dq_counts
    update_batch_dq_counts >> load_silver_quarantine
    load_silver_quarantine >> check_sales_dq_alerts

    # Email notification and transformations run independently
    check_sales_dq_alerts >> [
        notify_sales_dq,
        run_sales_dbt,
    ]
    run_sales_dbt >> complete_batch