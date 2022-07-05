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

-- via SnowSQL https://community.snowflake.com/s/question/0D50Z00007r3NloSAE/the-command-is-not-supported-from-the-ui-put
CREATE OR REPLACE STAGE users_stage;
PUT file:///tmp/snowdata/usersdata.csv @users_stage; -- stage the file in an internal LOCATION
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
CREATE OR REPLACE STAGE s3store URL='s3://snowflake-cookbook/Chapter02/r4/';
LIST @s3store;

CREATE OR REPLACE EXTERNAL TABLE tbl_ext WITH LOCATION = @s3store file_format = (TYPE=parquet);
SELECT * FROM tbl_ext LIMIT 10; -- external table always have JSON DATA

CREATE OR REPLACE EXTERNAL TABLE tbl_ext_csv WITH LOCATION = @s3store/csv pattern = '.*electronic-card-transactions-may-2020.csv' file_format = (TYPE=csv);
SELECT * FROM tbl_ext_csv LIMIT 10; -- external table always have JSON data


SHOW TABLES;

SELECT top 3
value:birthdate::date AS birth_date,
value:country::string AS country,
value:first_name::string AS first_name,
value:last_name::string AS last_name, 
value:salary::float AS salary
FROM tbl_ext;


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

