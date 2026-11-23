
MERGE INTO SALES_DB.DQ_SCM.SILVER_QUARANTINE AS target

USING (
    SELECT
        SHA2(
            CONCAT(
                SOURCE_FILE, '|',
                SOURCE_FILE_ROW_NUMBER::VARCHAR
            ),
            256
        ) AS SOURCE_ROW_ID,

        SOURCE_FILE,
        ORDER_ID,
        PRODUCT,
        QUANTITY,
        UNIT_PRICE,
        ORDER_DATE,
        ERROR_CODES

    FROM SALES_DB.SILVER_SCM.SALES_DQ_VALIDATION

    WHERE DQ_STATUS = 'INVALID'
      AND SOURCE_FILE IS NOT NULL
      AND SOURCE_FILE_ROW_NUMBER IS NOT NULL


    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SOURCE_FILE, SOURCE_FILE_ROW_NUMBER
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
        '{{ run_id }}',
        source.SOURCE_FILE,
        source.ORDER_ID,
        source.PRODUCT,
        source.QUANTITY,
        source.UNIT_PRICE,
        source.ORDER_DATE,
        source.ERROR_CODES,
        'OPEN'
    );