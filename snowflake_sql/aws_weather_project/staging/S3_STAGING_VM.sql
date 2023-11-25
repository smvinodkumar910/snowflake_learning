CREATE STAGE RAW_WEATHER_VM.S3_STAGING_VM
  STORAGE_INTEGRATION = AWS_S3_INTEG_VM
  URL = 's3://snowflake-stg-bucket-1/inbound_weather_data/'
 -- FILE_FORMAT = my_csv_format;