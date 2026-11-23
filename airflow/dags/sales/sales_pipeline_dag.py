from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="sales_pipeline",
    description="Sales Bronze to Gold data pipeline",
    schedule="0 1 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["sales", "snowflake", "dbt"],
) as dag:

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
            ON_ERROR = ABORT_STATEMENT;
        """,
    )

    run_sales_dbt = BashOperator(
    task_id="run_sales_dbt",
    bash_command=(
        "dbt run "
        "--project-dir /opt/airflow/dbt "
        "--profiles-dir /opt/airflow/config/dbt "
        "--select path:models/sales"
        ),
    )

    ingest_sales_bronze >> run_sales_dbt