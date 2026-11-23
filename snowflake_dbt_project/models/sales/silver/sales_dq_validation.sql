WITH source_data AS (
    SELECT *
    FROM {{ source('sales_bronze', 'RAW_SALES') }}
),

validate_records AS (
    SELECT
        *,

        -- Collect all validation failures for each record
        
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
