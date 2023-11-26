CREATE OR REPLACE PROCEDURE WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_WEATHER_DTL()
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$  
    var merge_statement =` MERGE INTO WEATHER_DB_VM.DM_WEATHER_VM.DM_WEATHER_DTL TGT
          USING
          (SELECT 
            CITY||'~'||TO_CHAR(LAST_UPDATED,'YYYYMMDDHHMI') AS INTEGRATION_ID,
            LAST_UPDATED_EPOCH,
            LAST_UPDATED,
            CITY,
            TEMP_C,
            TEMP_F,
            IS_DAY,
            WIND_MPH,
            WIND_KPH,
            WIND_DEGREE,
            WIND_DIR,
            PRESSURE_MB,
            PRESSURE_IN,
            PRECIP_MM,
            PRECIP_IN,
            HUMIDITY,
            CLOUD,
            FEELSLIKE_C,
            FEELSLIKE_F,
            VIS_KM,
            VIS_MILES,
            UV,
            GUST_MPH,
            GUST_KPH,
            CURRENT_TIMESTAMP() AS TGT_INSERT_DT,
            CURRENT_TIMESTAMP() AS TGT_UPDATE_DT
          FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_WEATHER_DTL
          WHERE EXISTS (SELECT 1 FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_WEATHER_DTL_STREAM )
          ) SRC ON (TGT.integration_id = src.integration_id)
          WHEN MATCHED THEN UPDATE SET
            TGT.INTEGRATION_ID = SRC.INTEGRATION_ID,
            TGT.LAST_UPDATED_EPOCH = SRC.LAST_UPDATED_EPOCH,
            TGT.LAST_UPDATED = SRC.LAST_UPDATED,
            TGT.CITY = SRC.CITY,
            TGT.TEMP_C = SRC.TEMP_C,
            TGT.TEMP_F = SRC.TEMP_F,
            TGT.IS_DAY = SRC.IS_DAY,
            TGT.WIND_MPH = SRC.WIND_MPH,
            TGT.WIND_KPH = SRC.WIND_KPH,
            TGT.WIND_DEGREE = SRC.WIND_DEGREE,
            TGT.WIND_DIR = SRC.WIND_DIR,
            TGT.PRESSURE_MB = SRC.PRESSURE_MB,
            TGT.PRESSURE_IN = SRC.PRESSURE_IN,
            TGT.PRECIP_MM = SRC.PRECIP_MM,
            TGT.PRECIP_IN = SRC.PRECIP_IN,
            TGT.HUMIDITY = SRC.HUMIDITY,
            TGT.CLOUD = SRC.CLOUD,
            TGT.FEELSLIKE_C = SRC.FEELSLIKE_C,
            TGT.FEELSLIKE_F = SRC.FEELSLIKE_F,
            TGT.VIS_KM = SRC.VIS_KM,
            TGT.VIS_MILES = SRC.VIS_MILES,
            TGT.UV = SRC.UV,
            TGT.GUST_MPH = SRC.GUST_MPH,
            TGT.GUST_KPH = SRC.GUST_KPH,
            TGT.TGT_INSERT_DT = SRC.TGT_INSERT_DT,
            TGT.TGT_UPDATE_DT = SRC.TGT_UPDATE_DT
        WHEN NOT MATCHED THEN INSERT 
        (INTEGRATION_ID,
        LAST_UPDATED_EPOCH,
        LAST_UPDATED,
        CITY,
        TEMP_C,
        TEMP_F,
        IS_DAY,
        WIND_MPH,
        WIND_KPH,
        WIND_DEGREE,
        WIND_DIR,
        PRESSURE_MB,
        PRESSURE_IN,
        PRECIP_MM,
        PRECIP_IN,
        HUMIDITY,
        CLOUD,
        FEELSLIKE_C,
        FEELSLIKE_F,
        VIS_KM,
        VIS_MILES,
        UV,
        GUST_MPH,
        GUST_KPH,
        TGT_INSERT_DT,
        TGT_UPDATE_DT
        )
        VALUES
        (SRC.INTEGRATION_ID,
        SRC.LAST_UPDATED_EPOCH,
        SRC.LAST_UPDATED,
        SRC.CITY,
        SRC.TEMP_C,
        SRC.TEMP_F,
        SRC.IS_DAY,
        SRC.WIND_MPH,
        SRC.WIND_KPH,
        SRC.WIND_DEGREE,
        SRC.WIND_DIR,
        SRC.PRESSURE_MB,
        SRC.PRESSURE_IN,
        SRC.PRECIP_MM,
        SRC.PRECIP_IN,
        SRC.HUMIDITY,
        SRC.CLOUD,
        SRC.FEELSLIKE_C,
        SRC.FEELSLIKE_F,
        SRC.VIS_KM,
        SRC.VIS_MILES,
        SRC.UV,
        SRC.GUST_MPH,
        SRC.GUST_KPH,
        SRC.TGT_INSERT_DT,
        SRC.TGT_UPDATE_DT
        )`;
    
    var merge = snowflake.createStatement( {sqlText: merge_statement} );
    var result_set1 = merge.execute();
    
    var truncate = snowflake.createStatement( {sqlText: 'TRUNCATE TABLE WEATHER_DB_VM.RAW_WEATHER_VM.RAW_WEATHER_DTL;'} );
    var result = truncate.execute();
$$
;