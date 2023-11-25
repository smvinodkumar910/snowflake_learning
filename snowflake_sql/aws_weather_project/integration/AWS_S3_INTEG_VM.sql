CREATE STORAGE INTEGRATION AWS_S3_INTEG_VM
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::128622952799:role/snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-stg-bucket-1/inbound_weather_data/')
  ---[ STORAGE_BLOCKED_LOCATIONS = ('s3://<bucket>/<path>/', 's3://<bucket>/<path>/') ]
