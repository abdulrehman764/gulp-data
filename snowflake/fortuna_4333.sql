USE ROLE accountadmin;
USE WAREHOUSE opahlab_warehouse;



CREATE DATABASE IF NOT EXISTS fortuna_3334;
USE DATABASE fortuna_3334;




-- FILE FORMAT WITH PARSE_HEADER - TO BE USED FOR CREATING TABLES
CREATE OR REPLACE FILE FORMAT fortuna_3334.public.EDA_CSV_PARSE_HEADER_FF
 TYPE = CSV
 PARSE_HEADER = TRUE
 FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
;





-- CREATE STORAGE INTEGRATION s3_int
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484850288072:role/mysnowflakerole'
--   STORAGE_ALLOWED_LOCATIONS = ('*');



-- DESC INTEGRATION s3_int_1;



-- SHOW STORAGE INTEGRATIONS;



-- GRANT CREATE STAGE ON SCHEMA public TO ROLE accountadmin;

-- GRANT USAGE ON INTEGRATION s3_int TO ROLE accountadmin;



-- CREATE STORAGE INTEGRATION s3_int_1
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::484850288072:role/mysnowflakerole'
--   STORAGE_ALLOWED_LOCATIONS = ('*');



CREATE STAGE my_s3_stage_1
  STORAGE_INTEGRATION = s3_int_1
  URL = 's3://gulp-data-vault-decode/'
  FILE_FORMAT = fortuna_3334.public.EDA_CSV_PARSE_HEADER_FF;


  SHOW STAGES;



  SELECT * FROM TABLE(
    INFER_SCHEMA ( 
        LOCATION=>'@my_s3_stage_1/fortuna-3334-32GB/fortuna.csv'
        , FILE_FORMAT=>'fortuna_3334.public.EDA_CSV_PARSE_HEADER_FF'
        , IGNORE_CASE=>TRUE
        , MAX_RECORDS_PER_FILE => 100
        )
    ) limit 10;




-- CREATE TABLE BASED ON INFERRED DETAIILS
CREATE OR REPLACE TABLE fortuna
    USING TEMPLATE (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA (
                    LOCATION=>'@my_s3_stage_1/fortuna-3334-32GB/fortuna.csv'
                    , FILE_FORMAT=>'fortuna_3334.public.EDA_CSV_PARSE_HEADER_FF'
                    , IGNORE_CASE=>TRUE
                    , MAX_RECORDS_PER_FILE => 1000
                )
            )
    );

select * from fortuna;

COPY INTO fortuna_3334.public.fortuna
FROM @my_s3_stage_1/fortuna-3334-32GB/fortuna.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);




select * from fortuna limit 100;




ALTER DATABASE IF EXISTS fortuna RENAME TO opahlab;


-- Suspend the warehouse
ALTER WAREHOUSE OPAHLAB_WAREHOUSE SUSPEND;


-- Check the status of the warehouse to confirm it is suspended
SHOW WAREHOUSES LIKE 'OPAHLAB_WAREHOUSE';





-- blitz

	
s3://gulp-data-vault-decode/6724/


  SELECT * FROM TABLE(
    INFER_SCHEMA ( 
        LOCATION=>'@my_s3_stage_1/6724/2024-01-24T211307.312Z/user_emails.csv'
        , FILE_FORMAT=>'opahlab.public.EDA_CSV_PARSE_HEADER_FF'
        , IGNORE_CASE=>TRUE
        , MAX_RECORDS_PER_FILE => 100
        )
    ) limit 10;








  CREATE TABLE IF NOT EXISTS opahlab.public.blitz (
    email STRING,
    location STRING,
    birthday STRING,
    device STRING
);

COPY INTO opahlab.public.blitz
FROM @my_s3_stage_1/6724/2024-01-24T211307.312Z/user_emails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1,error_on_column_count_mismatch=false  );



select distinct count(*) from blitz limit 100 --12846165


COPY INTO opahlab.public.blitz
FROM @my_s3_stage_1/6724/2024-01-29T221716.408Z/user_emails.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1,error_on_column_count_mismatch=false  );

select distinct count(*) from blitz limit 100 --13844946




COPY INTO opahlab.public.blitz
FROM @my_s3_stage_1/6724/2024-02-06T072818.883Z/emails1.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1,error_on_column_count_mismatch=false  );

select distinct count(*) from blitz limit 100 --26691893


COPY INTO opahlab.public.blitz
FROM @my_s3_stage_1/6724/2024-02-11T093102.650Z/operation_emails_reformatting.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1,error_on_column_count_mismatch=false  );

select distinct count(*) from blitz limit 100 --39538835











