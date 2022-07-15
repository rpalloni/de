/* share data with non-snowflake users */

-- [login as provider] create a reader account (child) in the provider account (parent)
USE ROLE ACCOUNTADMIN;

CREATE MANAGED ACCOUNT SH_NON_SF_ACCOUNT
TYPE=READER,
ADMIN_NAME = '<name>',
ADMIN_PASSWORD='<password>';

SHOW MANAGED ACCOUNTS; -- account url and locator

-- [login as reader] create a WH in reader account (no WH by default)
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE SP_VW WITH
WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

--grant ALL privileges to the SYSADMIN role to manage WH
GRANT ALL ON WAREHOUSE SP_VW TO ROLE SYSADMIN;

-- grant the USAGE privileges to the PUBLIC role (any user in reader can use WH)
GRANT USAGE ON WAREHOUSE SP_VW TO ROLE PUBLIC;

-- [login as provider] create a table in the provider account
CREATE DATABASE CUSTOMER_DATA;
CREATE OR REPLACE TABLE STORE_SALES AS
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES LIMIT 1000;

--create a secure view on top of the table
CREATE OR REPLACE SECURE VIEW STORE_SALES_AGG AS
SELECT SS_SOLD_DATE_SK, SUM(SS_NET_PROFIT) PROFIT
FROM CUSTOMER_DATA.PUBLIC.STORE_SALES
GROUP BY SS_SOLD_DATE_SK;

-- create share object as admin
USE ROLE ACCOUNTADMIN;
CREATE SHARE share_sales_data;

-- grant usage to share object on DB and schema containing the secure view
GRANT USAGE ON DATABASE CUSTOMER_DATA TO SHARE share_sales_data;
GRANT USAGE ON SCHEMA CUSTOMER_DATA.public TO SHARE share_sales_data;
GRANT SELECT ON VIEW CUSTOMER_DATA.PUBLIC.STORE_SALES_AGG TO SHARE share_sales_data; -- add the view to the share with SELECT permissions

-- add the reader to the share
ALTER SHARE share_sales_data ADD ACCOUNT = <reader_account_name>; -- rd12345


-- [login as reader] consume share
USE ROLE ACCOUNTADMIN;
SHOW SHARES; -- list inbound share related to account

-- db, schema and table are in the share but without a db attached <DB>.PUBLIC.STORE_SALES_AGG
DESCRIBE SHARE <provider_account_name>.share_sales_data; -- pr67890.share_sales_data

-- attach a database (reader side) via share
CREATE DATABASE SHR_SALES
FROM SHARE <provider_account_name>.share_sales_data;

DESCRIBE SHARE <provider_account_name>.share_sales_data; -- db set

SELECT * FROM SHR_SALES.PUBLIC.STORE_SALES_AGG;
