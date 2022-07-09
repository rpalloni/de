-- Data Definition Language: CREATE, ALTER, DROP, etc

USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE etl_wh;

USE WAREHOUSE etl_wh;

CREATE DATABASE cookbook;

SHOW DATABASES LIKE 'cookbook';

USE DATABASE cookbook;

select current_warehouse(), current_database(), current_schema();



CREATE TABLE USERS (
	id STRING,
	username STRING,
	age NUMBER
);

SELECT * FROM users;

INSERT INTO USERS values('003', 'cuser', 26);

-- via SnowSQL, it is not possible to use web ui
-- a tool that has access to the local file system is needed when loading from local file system
CREATE OR REPLACE STAGE users_stage;
PUT file:///tmp/data/usersdata.csv @users_stage; -- stage the file in an internal LOCATION
LIST @users_stage; -- verify the list of files staged successfully
COPY INTO users; -- copy into the table



CREATE OR REPLACE TABLE customers (
	id INT NOT NULL,
	last_name VARCHAR(100),
	first_name VARCHAR(100),
	email VARCHAR(100),
	compamy VARCHAR(100),
	phone VARCHAR(50),
	address1 STRING,
	address2 STRING,
	city VARCHAR(100),
	state VARCHAR(100),
	postal_code VARCHAR(20),
	country VARCHAR(50)
);

DESCRIBE TABLE customers;

COPY INTO customers FROM s3://snowflake-cookbook/Chapter02/r3/customer.csv
FILE_FORMAT = (TYPE = csv SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


SELECT * FROM customers;

-- deep and shallow copies
CREATE TABLE customer_deep_copy AS SELECT * FROM customers; -- structure and data
CREATE TABLE customer_shlw_copy LIKE customers; -- only structure

-- temporary and transient
CREATE TEMPORARY TABLE customer_temp AS SELECT * FROM customers WHERE try_to_number(postal_code) IS NOT NULL;
CREATE TRANSIENT TABLE customer_trans AS SELECT * FROM customers WHERE try_to_number(postal_code) IS NULL;

-- external
CREATE OR REPLACE STAGE s3store URL='s3://snowflake-cookbook/Chapter02/r4/'; -- public bucket
LIST @s3store;


CREATE OR REPLACE EXTERNAL TABLE tbl_ext 
WITH LOCATION = @s3store 
FILE_FORMAT = (TYPE=parquet);
SELECT * FROM tbl_ext LIMIT 10; -- external table always have JSON data

CREATE OR REPLACE EXTERNAL TABLE tbl_ext_csv 
WITH LOCATION = @s3store/csv pattern = '.*electronic-card-transactions-may-2020.csv' 
FILE_FORMAT = (TYPE=csv DATE_FORMAT = 'YYYY-MM-DD');
SELECT * FROM tbl_ext_csv LIMIT 10; -- external table always have JSON data


SHOW TABLES;

SELECT top 3
value:birthdate::date AS birth_date,
value:country::string AS country,
value:first_name::string AS first_name,
value:last_name::string AS last_name,
value:salary::float AS salary
FROM tbl_ext;


-- query stage directly
CREATE FILE FORMAT CUSTOM_PARQUET
TYPE = PARQUET;

CREATE OR REPLACE STAGE s3storeparquet 
URL='s3://snowflake-cookbook/Chapter02/r4/parquet/userdata1.parquet' 
FILE_FORMAT=CUSTOM_PARQUET;
LIST @s3storeparquet;

-- parquet data are loaded as JSON in SnowFlake (VARIANT data type column)
select $1 from @s3storeparquet;
select $1:birthdate from @s3storeparquet;

select PARSE_JSON($1) from @s3storeparquet;



-- simple and materialized views
CREATE DATABASE test_view_creation;
USE DATABASE test_view_creation;


CREATE VIEW test_view_creation.public.date_wise_orders as
select
l_commitdate as order_date,
sum(l_quantity) as tot_qty,
sum(l_extendedprice) as tot_price
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM
group by l_commitdate;



CREATE MATERIALIZED VIEW test_view_creation.public.date_wise_orders_m as
select
l_commitdate as order_date,
sum(l_quantity) as tot_qty,
sum(l_extendedprice) as tot_price
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM
group by l_commitdate;


-- access s3 bucket via snowflake with storage integration object
-- a storage integration allows users to avoid supplying credentials to access a private storage location
CREATE STORAGE INTEGRATION S3_INTEGRATION
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::1234567891012:role/SnowflakeRole'
STORAGE_ALLOWED_LOCATIONS = ('s3://abc123');

DESC INTEGRATION S3_INTEGRATION; -- change param in AWS Role Trust Relationship
-- STORAGE_AWS_IAM_USER_ARN
-- STORAGE_AWS_EXTERNAL_ID


CREATE FILE FORMAT S3CSV
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE S3_STAGE
STORAGE_INTEGRATION = S3_INTEGRATION
URL = 's3://abc123'
FILE_FORMAT = S3CSV;

LIST @S3_STAGE;


CREATE TABLE CARDSDATA (
	customer_name string,
	credit_card string,
	type string,
	cvv integer,
	exp_date string
);

COPY INTO CARDSDATA FROM @S3_STAGE;

select * from cardsdata;

-- direct reference
CREATE TABLE CARDSDATADIR (
	customer_name string,
	credit_card string,
	type string,
	cvv integer,
	exp_date string
);

COPY INTO cardsdatadir FROM s3://abc123/cc_info.csv
CREDENTIALS = (AWS_KEY_ID = '<string>' AWS_SECRET_KEY = '<string>') -- awsuser credentials
FILE_FORMAT = (TYPE = csv SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


select * from cardsdatadir;


-- export dwh data to S3
CREATE OR REPLACE STAGE EXPORT_DATA
STORAGE_INTEGRATION = S3_INTEGRATION 
URL = 's3://abc123'
FILE_FORMAT = (TYPE = CSV);

COPY INTO @EXPORT_DATA/cardsdata_backup.csv
FROM (
	SELECT * FROM CARDSDATA
);
-- cardsdata_backup.csv_0_0_0.csv.gz in bucket


