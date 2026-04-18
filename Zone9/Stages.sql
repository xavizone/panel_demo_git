--Truck Reviews Stage
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE STORAGE INTEGRATION int_tastybytes_truckreviews
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::981304421142:role/tb-de-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://sfquickstarts/');


  
CREATE OR REPLACE FILE FORMAT sfpd_tb_db.public.ff_csv
    TYPE = 'csv'
    SKIP_HEADER = 1   
    FIELD_DELIMITER = '|';

CREATE OR REPLACE STAGE sfpd_tb_db.public.stg_truck_reviews
    STORAGE_INTEGRATION = int_tastybytes_truckreviews
    URL = 's3://sfquickstarts/'
    FILE_FORMAT = public.ff_csv;

show integrations;
describe integration int_tastybytes_truckreviews;

SELECT TOP 100 METADATA$FILENAME,
       SPLIT_PART(METADATA$FILENAME, '/', 4) as source_name,
       CONCAT(SPLIT_PART(METADATA$FILENAME, '/', 2),'/' ,SPLIT_PART(METADATA$FILENAME, '/', 3)) as path,
       $1 as order_id,
       $2 as Language,
       $3 as Truck_id,
       $4 as Review,
       $5 as Order_ID,
FROM @ice_pipe_demo_db.public.raw_truck_reviews_s3load/;

SELECT distinct $3 as Truck_id
FROM @ice_pipe_demo_db.public.raw_truck_reviews_s3load/;

list @sfpd_tb_db.public.s3load;
Select distinct metadata$filename from @sfpd_tb_db.public.s3load;
Select distinct CONCAT(SPLIT_PART(METADATA$FILENAME, '/', 2),'/' ,SPLIT_PART(METADATA$FILENAME, '/', 3)) from @sfpd_tb_db.public.s3load; 

CREATE OR REPLACE STAGE ice_pipe_demo_db.public.raw_tastybytes_s3load
COMMENT = 'TastyBytes full load'
url = 's3://sfquickstarts/tastybytes/'
file_format = sfpd_tb_db.public.csv_ff;

list @ice_pipe_demo_db.public.raw_tastybytes_s3load;

CREATE OR REPLACE STAGE ice_pipe_demo_db.public.raw_tastybytes_s3load
COMMENT = 'Pos and Customer'
url = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
file_format = sfpd_tb_db.public.csv_ff;

list @ice_pipe_demo_db.public.raw_tastybytes_s3load;


-- External Table
CREATE OR REPLACE EXTERNAL TABLE tasty_bytes_db.raw.ext_survey_data
(
    source varchar as SPLIT_PART(METADATA$FILENAME, '/', 4),
    quarter varchar as CONCAT(SPLIT_PART(METADATA$FILENAME, '/', 2),'/' ,SPLIT_PART(METADATA$FILENAME, '/', 3)),
    order_id variant as IFNULL((value:c1),-1),
    truck_id bigint as (IFNULL(value:c2::int,-1)),
    language varchar as (value:c3::varchar),
    review varchar as (value:c5::varchar),
    primary_city varchar as (value:c6::varchar),
    review_year int as (value:c8::int),
    review_month int as (value:c9::int)
)
PARTITION BY (quarter, source)
LOCATION = @tasty_bytes_db.raw.stg_truck_reviews/
AUTO_REFRESH = TRUE
FILE_FORMAT = tasty_bytes_db.raw.ff_csv
PATTERN ='.*truck_reviews.*[.]csv'
COMMENT = '{"origin":"sf_sit-is", "name":"voc", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":0, "source":"sql", "vignette":"iceberg"}}';


SELECT * FROM tasty_bytes_db.raw.ext_survey_data LIMIT 100;
SHOW EXTERNAL TABLES;
SELECT "notification_channel"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

/* Copy the ARN of the SQS queue for the external table in the notification_channel column and follow the below steps.

Log into the AWS Management Console.

Open your S3 bucket created in setup

Click the properties tab

Scroll to Event notifications and click Create event notification

Configure an event notification for your S3 bucket using the instructions provided in the Amazon S3 documentation. Complete the fields as follows:

Name: Name of the event notification (e.g. Auto-ingest Snowflake).

Events: Select All object create events and All object removal events.

Destination: Select SQS Queue.

SQS: Select Add SQS queue ARN.

SQS queue ARN: Paste the SQS queue name from the SHOW EXTERNAL TABLES output.

Click save changes and the external stage with auto-refresh is now configured!

When new or updated data files are added to the S3 bucket, the event notification informs Snowflake to scan them into the external table metadata.

*/
