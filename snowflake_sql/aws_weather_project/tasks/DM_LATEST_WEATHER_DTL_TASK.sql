CREATE OR REPLACE TASK WEATHER_DB_VM.DM_WEATHER_VM.DM_LATEST_WEATHER_DTL_TASK
  ---SCHEDULE = '10 MINUTE'
  ---ALLOW_OVERLAPPING_EXECUTION = FALSE
  ---SUSPEND_TASK_AFTER_NUM_FAILURES = 2 
  --[ ERROR_INTEGRATION = <integration_name> ]
  --[ COPY GRANTS ]
  --[ COMMENT = '<string_literal>' ]
  --AFTER WEATHER_DB_VM.DM_WEATHER_VM.DM_WEATHER_DTL_TASK,WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL_TASK
AS
  CALL WEATHER_DB_VM.DM_WEATHER_VM.SP_PLP_DM_LATEST_WEATHER_DTL();