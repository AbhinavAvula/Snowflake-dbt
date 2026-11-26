MERGE INTO SALES_DB.DQ_SCM.SILVER_QUARANTINE AS target

USING (
    SELECT
        SOURCE_ROW_ID,
        BATCH_ID,
        SOURCE_FILE,
        ORDER_ID,
        PRODUCT,
        QUANTITY,
        UNIT_PRICE,
        ORDER_DATE,
        ERROR_CODES

    FROM SALES_DB.SILVER_SCM.SALES_DQ_VALIDATION

    WHERE DQ_STATUS = 'INVALID'

      -- Process only the current Airflow batch
      AND BATCH_ID = (
          SELECT BATCH_ID
          FROM SALES_DB.CONTROL_SCM.PIPELINE_BATCHES
          WHERE DAG_ID = 'sales_pipeline'
            AND AIRFLOW_RUN_ID =
                '{{ run_id | replace("'", "''") }}'
      )

      AND SOURCE_ROW_ID IS NOT NULL

    -- Prevent duplicate source rows in a single MERGE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SOURCE_ROW_ID
        ORDER BY SOURCE_FILE
    ) = 1

) AS source

ON target.SOURCE_ROW_ID = source.SOURCE_ROW_ID

WHEN NOT MATCHED THEN
    INSERT (
        SOURCE_ROW_ID,
        BATCH_ID,
        SOURCE_FILE,
        ORDER_ID,
        PRODUCT,
        QUANTITY,
        UNIT_PRICE,
        ORDER_DATE,
        ERROR_CODES,
        STATUS
    )
    VALUES (
        source.SOURCE_ROW_ID,
        source.BATCH_ID,
        source.SOURCE_FILE,
        source.ORDER_ID,
        source.PRODUCT,
        source.QUANTITY,
        source.UNIT_PRICE,
        source.ORDER_DATE,
        source.ERROR_CODES,
        'OPEN'
    );
