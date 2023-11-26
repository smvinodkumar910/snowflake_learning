
--- copy history to show snow pipe / copy to statement runs
select DISTINCT PIPE_NAME from snowflake.account_usage.copy_history
where PIPE_NAME IS NOT NULL;


--- load history shows copy_to statement runs
select * from information_schema.load_history
WHERE TABLE_NAME='RAW_LOCATION_DTL'
ORDER BY LAST_LOAD_TIME DESC;


-- shows task run history
SELECT *
FROM snowflake.account_usage.task_history
WHERE SCHEMA_NAME='DM_WEATHER_VM'
AND NAME ='DM_CURRENT_CONDITION_DTL_TASK';
--ORDER BY QUERY_START_TIME DESC
--LIMIT 10;

--To view now pipe status -- latest file details 
select SYSTEM$PIPE_STATUS('RAW_CURRENT_CONDITION_DTL_PIPE');


--to view tasks available
SHOW TASKS IN WEATHER_DB_VM.DM_WEATHER_VM;


ALTER TASK WEATHER_DB_VM.DM_WEATHER_VM.DM_LOCATION_DTL_TASK SUSPEND ;

EXECUTE TASK WEATHER_DB_VM.DM_WEATHER_VM.DM_LOCATION_DTL_TASK;



--To view available streams
 SHOW STREAMS;



-- to see stream has data or not
SELECT SYSTEM$STREAM_HAS_DATA('WEATHER_DB_VM.RAW_WEATHER_VM.RAW_WEATHER_DTL_STREAM');


-- to view the stream data
SELECT * FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL_STREAM;

-- To refer information schema
SELECT COLUMN_NAME FROM WEATHER_DB_VM.INFORMATION_SCHEMA.COLUMNS
where table_schema='DM_WEATHER_VM' AND TABLE_NAME ='DM_CURRENT_CONDITION_DTL'
ORDER BY ORDINAL_POSITION;


SHOW INTEGRATIONS;
DESC INTEGRATION AWS_S3_INTEG_VM;
--STORAGE_AWS_IAM_USER_ARN
--STORAGE_AWS_EXTERNAL_ID
SHOW PIPES;

