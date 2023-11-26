CREATE OR REPLACE PROCEDURE WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_CURRENT_CONDITION_DTL()
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$  
    var merge_statement =` MERGE INTO WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL TGT
          USING
          (SELECT 
            CITY||'~'||TO_CHAR(WEATHER_TIME,'YYYYMMDDHHMI') AS INTEGRATION_ID,
            TEXT,
            ICON,
            CODE,
            WEATHER_TIME,
            CITY,
            CURRENT_TIMESTAMP() AS TGT_INSERT_DT,
            CURRENT_TIMESTAMP() AS TGT_UPDATE_DT
          FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL
          WHERE EXISTS (SELECT 1 FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL_STREAM )
          ) SRC ON (TGT.integration_id = src.integration_id)
          WHEN MATCHED THEN UPDATE SET
            TGT.INTEGRATION_ID = SRC.INTEGRATION_ID,
            TGT.TEXT = SRC.TEXT,
            TGT.ICON = SRC.ICON,
            TGT.CODE = SRC.CODE,
            TGT.WEATHER_TIME = SRC.WEATHER_TIME,
            TGT.CITY = SRC.CITY,
            TGT.TGT_INSERT_DT = SRC.TGT_INSERT_DT,
            TGT.TGT_UPDATE_DT = SRC.TGT_UPDATE_DT
        WHEN NOT MATCHED THEN INSERT 
        ( INTEGRATION_ID,
          TEXT,
          ICON,
          CODE,
          WEATHER_TIME,
          CITY,
          TGT_INSERT_DT,
          TGT_UPDATE_DT
        )
        VALUES
        (SRC.INTEGRATION_ID,
        SRC.TEXT,
        SRC.ICON,
        SRC.CODE,
        SRC.WEATHER_TIME,
        SRC.CITY,
        SRC.TGT_INSERT_DT,
        SRC.TGT_UPDATE_DT
        )`;
    
    var merge = snowflake.createStatement( {sqlText: merge_statement} );
    var result_set1 = merge.execute();

    var truncate = snowflake.createStatement( {sqlText: 'TRUNCATE TABLE WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL;'} );
    var result = truncate.execute();
    
$$
;