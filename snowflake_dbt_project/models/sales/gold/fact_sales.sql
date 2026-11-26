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
    PRODUCT,
    QUANTITY,
    UNIT_PRICE,
    ORDER_DATE,
    TOTAL_AMOUNT,

    -- Extract the sales year for downstream analytics
    YEAR(ORDER_DATE) AS YEAR

FROM {{ ref('clean_sales') }}

-- Process only records belonging to the current batch
WHERE BATCH_ID = (
    SELECT BATCH_ID
    FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
    WHERE DAG_ID = 'sales_pipeline'
      AND AIRFLOW_RUN_ID =
          '{{ var("airflow_run_id") | replace("'", "''") }}'
)
