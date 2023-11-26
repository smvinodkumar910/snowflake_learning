create or replace pipe WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL_PIPE auto_ingest=true as
    COPY INTO "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_CURRENT_CONDITION_DTL"
FROM (SELECT 
METADATA$FILENAME, 
METADATA$FILE_ROW_NUMBER, 
METADATA$FILE_CONTENT_KEY, 
METADATA$FILE_LAST_MODIFIED, 
METADATA$START_SCAN_TIME,
A.$1,
A.$2,
A.$3,
A.$4,
A.$5
FROM '@"WEATHER_DB_VM"."RAW_WEATHER_VM"."S3_STAGING_VM"' A)
--FILES = ('2023/11/25/13/CURRENT_CONDITION_DTL_20231125132746.csv')
PATTERN='.*CURRENT_CONDITION_DTL_[0-9]+.csv'
FILE_FORMAT = (FORMAT_NAME="WEATHER_DB_VM"."RAW_WEATHER_VM"."WEATHER_FILE_FORMAT");
--ON_ERROR=ABORT_STATEMENT;
  