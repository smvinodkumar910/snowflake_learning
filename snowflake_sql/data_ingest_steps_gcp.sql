SHOW ROLES;

--Creating a new Database
CREATE DATABASE IF NOT EXISTS MY_TRIAL_DB;

--Creating a new Schema in MY_TRIAL_DB
CREATE SCHEMA IF NOT EXISTS MY_TRIAL_DB.MY_TRIAL_SCHEMA;

--Creating a  new file format
USE SCHEMA MY_TRIAL_DB.MY_TRIAL_SCHEMA;
CREATE OR REPLACE FILE FORMAT SAMPLE_CSV_FORMAT
TYPE=CSV
SKIP_HEADER=1
DATE_FORMAT="mm/dd/yy"
TIME_FORMAT="mm/dd/yy hh:mi";

--Creating an internal snowflake stage
CREATE STAGE trial_snowflake_stg 
	DIRECTORY = ( ENABLE = true ) 
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' );
    --FILE_FORMAT = my_csv_format;  -- TO SPECIFY FILE_FORMAT in STG level

--- Load file from local stg into a table
COPY INTO "MY_TRIAL_DB"."MY_TRIAL_SCHEMA"."BILLIONARIES_DATA"
FROM '@"MY_TRIAL_DB"."MY_TRIAL_SCHEMA"."TRIAL_SNOWFLAKE_STG"'
FILES = ('Billionaires_Statistics_Dataset.csv.gz')
FILE_FORMAT = (FORMAT_NAME="MY_TRIAL_DB"."MY_TRIAL_SCHEMA"."SAMPLE_CSV_FORMAT")
ON_ERROR=ABORT_STATEMENT;

-- In case of external stage -- creating an integration
create storage integration gcs_bucket_integration
    type = external_stage
    storage_provider = gcs
    enabled = true
    storage_allowed_locations = ( 'gcs://snowflake_stg_area/inbound1' )
    -- storage_blocked_locations = ( 'gcs://<location1>', 'gcs://<location2>' )
    comment = 'creating a storage integration';

--create an external gcs stage
CREATE STAGE TRIAL_GCS_STG 
	URL = 'gcs://snowflake_stg_area/inbound1/' 
	STORAGE_INTEGRATION = GCS_BUCKET_INTEGRATION 
	DIRECTORY = ( ENABLE = true );
    --auto refresh has to be refer

-- To auto refresh the gcs bucket pub/sub notification
--gsutil notification create -t snowflake_gcs_event -f json gs://snowflake_stg_area

--Creating notification Integration
CREATE NOTIFICATION INTEGRATION gcs_notify_integration
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/mynewdevenv/subscriptions/snowflake_gcs_notification_sub';

  -- Altering the stage for auto refresh of the data in gcs bucket
  alter STAGE TRIAL_GCS_STG set 
	DIRECTORY = ( AUTO_REFRESH = true NOTIFICATION_INTEGRATION = 'GCS_NOTIFY_INTEGRATION' );
  
DESC STAGE trial_snowflake_stg;

DESC STORAGE INTEGRATION gcs_bucket_integration;

DESC NOTIFICATION INTEGRATION gcs_notify_integration;