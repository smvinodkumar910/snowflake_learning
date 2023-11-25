create table if not exists WEATHER_DB_VM.RAW_WEATHER_VM.RAW_LOCATION_DTL
(
filename varchar,
file_row_number int,
file_content_key varchar,
file_last_modified timestamp_ntz,
start_scan_time timestamp_ltz,
name VARCHAR(100),
region VARCHAR(100),
country VARCHAR(100),
lat FLOAT,
lon FLOAT,
tz_id VARCHAR(100),
localtime_epoch NUMBER,
city_localtime DATETIME,
CONSTRAINT pk_location_dtl primary key (name)
);