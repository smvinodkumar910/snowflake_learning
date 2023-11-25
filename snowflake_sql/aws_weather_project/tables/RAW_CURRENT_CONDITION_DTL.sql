create table if not exists WEATHER_DB_VM.RAW_WEATHER_VM.RAW_CURRENT_CONDITION_DTL
(
filename varchar,
file_row_number int,
file_content_key varchar,
file_last_modified timestamp_ntz,
start_scan_time timestamp_ltz,
text VARCHAR(100),
icon VARCHAR(500),
code NUMBER,
weather_time DATETIME,
city VARCHAR(100)
);
