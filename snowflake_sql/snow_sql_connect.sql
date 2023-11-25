SHOW ROLES;
--Command to connect snowsql
snowsql -a in24558.us-central1.gcp -u vinod

--Upload local file to named staging area
put file://C:\Users\smvin\Downloads\archive\Billionaires_Statistics_Dataset.csv @MY_TRIAL_DB.MY_TRIAL_SCHEMA.TRIAL_SNOWFLAKE_STG
--Upload local file to table's staging area
put file://C:\Users\smvin\Downloads\archive\Billionaires_Statistics_Dataset.csv @%MY_TRIAL_DB.MY_TRIAL_SCHEMA.TABLE_NAME
--Upload local file to user's staging area
put file://C:\Users\smvin\Downloads\archive\Billionaires_Statistics_Dataset.csv @~MY_TRIAL_DB.MY_TRIAL_SCHEMA.TABLE_NAME