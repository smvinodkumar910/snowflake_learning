CREATE OR REPLACE PROCEDURE WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_LOCATION_DTL()
RETURNS VARCHAR(100)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$  
    var merge_statement =` MERGE INTO WEATHER_DB_VM.DM_WEATHER_VM.DM_LOCATION_DTL TGT
          USING
          (SELECT 
          name as integration_id,
          name as city,
          region,
          country,
          lat,
          lon,
          tz_id,
          localtime_epoch,
          city_localtime,
          current_timestamp() as tgt_insert_dt,
          current_timestamp() as tgt_update_dt,
          TRY_TO_GEOGRAPHY('POINT('||lon||' '||lat||')') as city_location
          FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL
          WHERE EXISTS (SELECT 1 FROM WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL_STREAM )
          ) SRC ON (TGT.integration_id = src.integration_id)
          WHEN MATCHED THEN UPDATE SET
          TGT.INTEGRATION_ID = SRC.INTEGRATION_ID,
        TGT.CITY = SRC.CITY,
        TGT.REGION = SRC.REGION,
        TGT.COUNTRY = SRC.COUNTRY,
        TGT.LAT = SRC.LAT,
        TGT.LON = SRC.LON,
        TGT.TZ_ID = SRC.TZ_ID,
        TGT.LOCALTIME_EPOCH = SRC.LOCALTIME_EPOCH,
        TGT.CITY_LOCALTIME = SRC.CITY_LOCALTIME,
        TGT.TGT_INSERT_DT = SRC.TGT_INSERT_DT,
        TGT.TGT_UPDATE_DT = SRC.TGT_UPDATE_DT,
        TGT.CITY_LOCATION = SRC.CITY_LOCATION
        WHEN NOT MATCHED THEN INSERT 
        (INTEGRATION_ID,
        CITY,
        REGION,
        COUNTRY,
        LAT,
        LON,
        TZ_ID,
        LOCALTIME_EPOCH,
        CITY_LOCALTIME,
        TGT_INSERT_DT,
        TGT_UPDATE_DT,
        CITY_LOCATION)
        VALUES
        (SRC.INTEGRATION_ID,
        SRC.CITY,
        SRC.REGION,
        SRC.COUNTRY,
        SRC.LAT,
        SRC.LON,
        SRC.TZ_ID,
        SRC.LOCALTIME_EPOCH,
        SRC.CITY_LOCALTIME,
        SRC.TGT_INSERT_DT,
        SRC.TGT_UPDATE_DT,
        SRC.CITY_LOCATION)`;
    
    var merge = snowflake.createStatement( {sqlText: merge_statement} );
    var result_set1 = merge.execute();

    var truncate = snowflake.createStatement( {sqlText: 'TRUNCATE TABLE WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL;'} );
    var result = truncate.execute();
    
$$
;