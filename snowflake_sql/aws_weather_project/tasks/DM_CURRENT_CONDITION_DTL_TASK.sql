CREATE OR REPLACE TASK WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL_TASK
  AFTER WEATHER_DB_VM.DM_WEATHER_VM.DM_LOCATION_DTL_TASK
 WHEN SYSTEM$STREAM_HAS_DATA('WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL_STREAM')
AS
  CALL WEATHER_DB_VM.DM_WEATHER_VM.SP_SIL_DM_CURRENT_CONDITION_DTL();