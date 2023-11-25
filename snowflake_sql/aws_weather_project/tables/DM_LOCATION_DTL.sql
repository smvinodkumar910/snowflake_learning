create table if not exists WEATHER_DB_VM.DM_WEATHER_VM.DM_LOCATION_DTL
(
integration_id varchar(100),
city VARCHAR(100),
region VARCHAR(100),
country VARCHAR(100),
lat FLOAT,
lon FLOAT,
tz_id VARCHAR(100),
localtime_epoch NUMBER,
city_localtime DATETIME,
TGT_INSERT_DT DATETIME,
TGT_UPDATE_DT DATETIME,
city_location GEOGRAPHY,
CONSTRAINT pk_location_dtl primary key (integration_id)
);