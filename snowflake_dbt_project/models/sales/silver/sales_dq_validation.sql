
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='SOURCE_ROW_ID',
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (

    SELECT
        *,
        SHA2(
            SOURCE_FILE || '|' ||
            SOURCE_FILE_ROW_NUMBER::VARCHAR,
            256
        ) AS SOURCE_ROW_ID

    FROM {{ source('sales_bronze', 'RAW_SALES') }}

    -- Process only records belonging to this Airflow execution
    WHERE BATCH_ID = (
        SELECT BATCH_ID
        FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
        WHERE DAG_ID = 'sales_pipeline'
          AND AIRFLOW_RUN_ID =
              '{{ var("airflow_run_id") | replace("'", "''") }}'
    )
      AND SOURCE_FILE IS NOT NULL
      AND SOURCE_FILE_ROW_NUMBER IS NOT NULL
),

validate_records AS (

    SELECT
        *,

        -- Collect all business-rule violations
        ARRAY_CONSTRUCT_COMPACT(
            IFF(ORDER_ID IS NULL,
                'NULL_ORDER_ID', NULL),

            IFF(PRODUCT IS NULL OR TRIM(PRODUCT) = '',
                'INVALID_PRODUCT', NULL),

            IFF(QUANTITY IS NULL OR QUANTITY <= 0,
                'INVALID_QUANTITY', NULL),

            IFF(UNIT_PRICE IS NULL OR UNIT_PRICE < 0,
                'INVALID_UNIT_PRICE', NULL),

            IFF(ORDER_DATE IS NULL,
                'INVALID_ORDER_DATE', NULL)
        ) AS ERROR_CODES

    FROM source_data
)

SELECT
    *,
    IFF(
        ARRAY_SIZE(ERROR_CODES) = 0,
        'VALID',
        'INVALID'
    ) AS DQ_STATUS

FROM validate_records
