create table if not exists WEATHER_DB_VM.DM_WEATHER_VM.DM_CURRENT_CONDITION_DTL
(
integration_id VARCHAR(200),
text VARCHAR(100),
icon VARCHAR(500),
code NUMBER,
weather_time DATETIME,
city VARCHAR(100),
TGT_INSERT_DT DATETIME,
TGT_UPDATE_DT DATETIME
);
