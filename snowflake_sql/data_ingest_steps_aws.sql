--CREATE STORAGE INTEGRATION


DESC INTEGRATION AWS_S3_INTEG_VM;
--STORAGE_AWS_IAM_USER_ARN
--STORAGE_AWS_EXTERNAL_ID

SHOW INTEGRATIONS;

SELECT COLUMN_NAME FROM WEATHER_DB_VM.INFORMATION_SCHEMA.COLUMNS
where table_schema='DM_WEATHER_VM' AND TABLE_NAME ='DM_CURRENT_CONDITION_DTL'
ORDER BY ORDINAL_POSITION;



 CALL "SP_SIL_DM_LOCATION_DTL"();

 TRUNCATE TABLE WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL;

 SELECT * FROM WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL;

 CALL SP_SIL_DM_CURRENT_CONDITION_DTL();



CREATE OR REPLACE TABLE DM_WEATHER_VM.DM_LATEST_WEATHER_DTL AS
SELECT WD.CITY||'~'||TO_CHAR(WEATHER_TIME,'YYYYMMDD')AS INTEGRATION_ID, WD.CITY,WD.TEMP_C, WD.CLOUD, CC.TEXT, CC.ICON, CC.WEATHER_TIME, LD.CITY_LOCATION, LD.CITY_LOCALTIME ,
WEATHER_TIME AS EFFECTIVE_FROM_DATE,
NULL AS EFFECTIVE_TO_DATE,
'Y' AS ACTIVE_REC_FLAG,
CURRENT_TIMESTAMP AS TGT_INSERT_DT,
CURRENT_TIMESTAMP AS TGT_UPDATE_DT
FROM DM_WEATHER_VM.DM_WEATHER_DTL WD
LEFT JOIN DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL CC ON WD.INTEGRATION_ID = CC.INTEGRATION_ID
LEFT JOIN DM_WEATHER_VM.DM_LOCATION_DTL LD ON WD.CITY = LD.INTEGRATION_ID;