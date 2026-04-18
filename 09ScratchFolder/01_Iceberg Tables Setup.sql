USE ROLE SYSADMIN;
ALTER SESSION SET query_tag = '{"Phase":"Setup","Project":"Panel Demo","version":{"major":1, "minor":1},"Organization":"TastyBytes", "source":"setup-sfpd.sql", "Section": "1","Goal":"Setting up Iceberg Tables,Connection to an external Volume"}';


--====Iceberg requires an external VOLUME
-- For this demo I am using my sandbox AWS bucket with required IAM permissions
-- Lets start with External VOLUME

  
-- STEP 1-- Connecting an External Volume where my Data resides
-- Allowing me to work with my data in Open Format without ingesting in Snowflake
-- With full Snowflake Warehouse performance 

// Setting up my external volume
// I can work on my data 

USE role accountadmin;

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'tb-de-demo-bucket'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://tb-de-demo-bucket'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::981304421142:role/tb-de-role'
            STORAGE_AWS_EXTERNAL_ID = 'iceberg_table_external_id'
         )
      )
      ALLOW_WRITES = TRUE;

//DESCRIBE external volume iceberg_external_volume;
DESC EXTERNAL VOLUME iceberg_external_volume;

/* OUTPUT 
{"NAME":"tb-de-demo-bucket","STORAGE_PROVIDER":"S3","STORAGE_BASE_URL":"s3://tb-de-demo-bucket","STORAGE_ALLOWED_LOCATIONS":["s3://tb-de-demo-bucket/*"],"STORAGE_AWS_ROLE_ARN":"arn:aws:iam::981304421142:role/tb-de-role","STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::518289065437:user/ecsm1000-s","STORAGE_AWS_EXTERNAL_ID":"iceberg_table_external_id","ENCRYPTION_TYPE":"NONE","ENCRYPTION_KMS_KEY_ID":""} */

//	Replaced		    "AWS": "arn:aws:iam::981304421142:root"

DESC EXTERNAL VOLUME iceberg_external_volume;
SELECT 
    PARSE_JSON("property_value"):STORAGE_AWS_IAM_USER_ARN AS STORAGE_AWS_IAM_USER_ARN,
    PARSE_JSON("property_value"):STORAGE_AWS_EXTERNAL_ID AS STORAGE_AWS_EXTERNAL_ID,
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "property" = 'STORAGE_LOCATION_1';

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('iceberg_external_volume');

/* OUTPUT 
{"success":true,"storageLocationSelectionResult":"PASSED","storageLocationName":"tb-de-demo-bucket","servicePrincipalProperties":"STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::518289065437:user/ecsm1000-s; STORAGE_AWS_EXTERNAL_ID: iceberg_table_external_id","location":"s3://tb-de-demo-bucket","storageAccount":null,"region":"us-east-2","writeResult":"PASSED","readResult":"PASSED","listResult":"PASSED","deleteResult":"PASSED","awsRoleArnValidationResult":"PASSED","azureGetUserDelegationKeyResult":"SKIPPED"}
*/


CREATE OR REPLACE DATABASE ice_pipe_demo_db;

CREATE OR REPLACE WAREHOUSE ice_pipe_demo_wh
    WAREHOUSE_SIZE = 'large' -- Large for initial data load - scaled down to XSmall at end of this scripts
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'Tasty Bytes - Data Engineering Warehouse';

CREATE OR REPLACE SCHEMA ice_pipe_demo_db.raw_data_schema;
USE DATABASE ice_pipe_demo_db;
USE WAREHOUSE ice_pipe_demo_wh;
use schema ice_pipe_demo_db.raw_data_schema;

--select current_user();
CREATE ROLE sandbox_role;
GRANT ALL ON DATABASE ice_pipe_demo_db TO ROLE sandbox_role WITH GRANT OPTION;
GRANT ALL ON SCHEMA ice_pipe_demo_db.raw_data_schema TO ROLE sandbox_role WITH GRANT OPTION;
GRANT ALL ON WAREHOUSE ice_pipe_demo_wh TO ROLE sandbox_role WITH GRANT OPTION;
GRANT ALL ON FUTURE TABLES IN SCHEMA ice_pipe_demo_db.raw_data_schema TO ROLE sandbox_role;
GRANT ALL ON FUTURE VIEWS IN SCHEMA ice_pipe_demo_db.raw_data_schema TO ROLE sandbox_role;

SET my_user = CURRENT_USER();
GRANT ROLE sandbox_role TO USER IDENTIFIER($my_user);

USE ROLE sandbox_role;





--
CREATE OR REPLACE ICEBERG TABLE ice_pipe_demo_db.raw_data_schema.TRUCK_REVIEWS (
    source_name VARCHAR,
    quarter VARCHAR,
    order_id BIGINT,
    truck_id INT,
    review VARCHAR,
    language VARCHAR
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'iceberg_external_volume'
    BASE_LOCATION = 'raw_data/iceberg_only/'
;

CREATE OR REPLACE STAGE ice_pipe_demo_db.public.raw_truck_reviews_s3load
COMMENT = 'Raw POS'
url = 's3://sfquickstarts/tastybytes-voc/raw_support/truck_reviews/'
file_format = sfpd_tb_db.public.csv_ff;

list @ice_pipe_demo_db.public.raw_truck_reviews_s3load;
list @sfpd_tb_db.public.s3load;
list @stg_truck_reviews;