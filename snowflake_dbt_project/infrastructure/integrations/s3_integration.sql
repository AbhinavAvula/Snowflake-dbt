CREATE STORAGE INTEGRATION IF NOT EXISTS s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::787124622890:role/snowflake-dbt-s3-role'
STORAGE_ALLOWED_LOCATIONS = (
    's3://snowflake-dbt-s3-bkt/sales/bronze/'
);