{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SOURCE_ROW_ID',
        on_schema_change='append_new_columns'
    )
}}

SELECT
    SOURCE_ROW_ID,
    BATCH_ID,
    ORDER_ID,
    TRIM(PRODUCT) AS PRODUCT,
    QUANTITY,
    UNIT_PRICE,
    ORDER_DATE,
    QUANTITY * UNIT_PRICE AS TOTAL_AMOUNT

FROM {{ ref('sales_dq_validation') }}

WHERE DQ_STATUS = 'VALID'

-- Process only the current Airflow batch
AND BATCH_ID = (
    SELECT BATCH_ID
    FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
    WHERE DAG_ID = 'sales_pipeline'
      AND AIRFLOW_RUN_ID =
          '{{ var("airflow_run_id") | replace("'", "''") }}'
)
