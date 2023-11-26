create stream WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL_STREAM
    --copy grants
    on table WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL
    -- { at | before } { timestamp => <timestamp> | offset => <time_difference> | statement => <id> }
    append_only = true 
    -- comment = '<comment>'
    ;