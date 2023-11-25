
TRUNCATE TABLE "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_LOCATION_DTL";
--DROP TABLE "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_LOCATION_DTL";
COPY INTO "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_LOCATION_DTL"
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
A.$5,
A.$6,
A.$7,
A.$8
FROM '@"WEATHER_DB_VM"."RAW_WEATHER_VM"."S3_STAGING_VM"/2023/11/25/13/LOCATION_DTL_20231125132746.csv' A)
--FILES = ('2023/11/25/13/LOCATION_DTL_20231125132746.csv')
--PATTERN='[\d\/]+LOCATION_DTL_[0-9]+.csv'
FILE_FORMAT = (FORMAT_NAME="WEATHER_DB_VM"."RAW_WEATHER_VM"."WEATHER_FILE_FORMAT")
ON_ERROR=ABORT_STATEMENT;



TRUNCATE TABLE "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_WEATHER_DTL";
--DROP TABLE WEATHER_DB_VM.RAW_WEATHER_VM.RAW_WEATHER_DTL;
COPY INTO "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_WEATHER_DTL"
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
A.$5,
A.$6,
A.$7,
A.$8,
A.$9,
A.$10,
A.$11,
A.$12,
A.$13,
A.$14,
A.$15,
A.$16,
A.$17,
A.$18,
A.$19,
A.$20,
A.$21,
A.$22,
A.$23
FROM '@"WEATHER_DB_VM"."RAW_WEATHER_VM"."S3_STAGING_VM"/2023/11/25/13/WEATHER_DETAILS_DTL_20231125132746.csv' A)
--FILES = ('2023/11/25/13/WEATHER_DETAILS_DTL_20231125132746.csv')
--PATTERN='[\d\/]+LOCATION_DTL_[0-9]+.csv'
FILE_FORMAT = (FORMAT_NAME="WEATHER_DB_VM"."RAW_WEATHER_VM"."WEATHER_FILE_FORMAT")
ON_ERROR=ABORT_STATEMENT;



TRUNCATE TABLE "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_CURRENT_CONDITION_DTL";
--DROP TABLE "WEATHER_DB_VM"."RAW_WEATHER_VM"."RAW_CURRENT_CONDITION_DTL";
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
FROM '@"WEATHER_DB_VM"."RAW_WEATHER_VM"."S3_STAGING_VM"/2023/11/25/13/CURRENT_CONDITION_DTL_20231125132746.csv' A)
--FILES = ('2023/11/25/13/CURRENT_CONDITION_DTL_20231125132746.csv')
--PATTERN='[\d\/]+LOCATION_DTL_[0-9]+.csv'
FILE_FORMAT = (FORMAT_NAME="WEATHER_DB_VM"."RAW_WEATHER_VM"."WEATHER_FILE_FORMAT")
ON_ERROR=ABORT_STATEMENT;



CALL WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_CURRENT_CONDITION_DTL();

CALL WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_LOCATION_DTL();

CALL WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_WEATHER_DTL();

USE SCHEMA WEATHER_DB_VM.RAW_WEATHER_VM;
select *
from table(information_schema.copy_history(TABLE_NAME=>'RAW_CURRENT_CONDITION_DTL', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

select * from information_schema.load_history;

