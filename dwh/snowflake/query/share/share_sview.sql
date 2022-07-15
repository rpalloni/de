/* share secure view with another snowflake account */

CREATE DATABASE CUSTOMER_DB;

CREATE TABLE CUSTOMER (
  CUST_ID NUMBER,
  CUST_NAME STRING
);

INSERT INTO CUSTOMER
SELECT
    SEQ8() AS CUST_ID,
    RANDSTR(10,RANDOM()) AS CUST_NAME
FROM TABLE(GENERATOR(ROWCOUNT => 1000));


CREATE DATABASE ADDRESS_DB;

CREATE TABLE ADDRESS (
  CUST_ID NUMBER,
  CUST_ADDRESS STRING
);

INSERT INTO ADDRESS
SELECT
    SEQ8() AS CUST_ID,
    RANDSTR(50,RANDOM()) AS CUST_ADDRESS
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- db with a SECURE VIEW with join on customer and address
CREATE DATABASE VIEW_SHR_DB;

CREATE SECURE VIEW CUSTOMER_INFO AS
SELECT
  CUS.CUST_ID,
  CUS.CUST_NAME,
  ADS.CUST_ADDRESS
FROM CUSTOMER_DB.PUBLIC.CUSTOMER CUS
INNER JOIN ADDRESS_DB.PUBLIC.ADDRESS ADS
ON CUS.CUST_ID = ADS.CUST_ID;

-- validate that the view is working
SELECT * FROM CUSTOMER_INFO;

-- create share object as admin
USE ROLE ACCOUNTADMIN;
CREATE SHARE share_cust_data;

-- grant usage to share object on DB and schema containing the secure view
GRANT USAGE ON DATABASE VIEW_SHR_DB TO SHARE share_cust_data;
GRANT USAGE ON SCHEMA VIEW_SHR_DB.public TO SHARE share_cust_data;

-- WARNING: also grant reference_usage on all databases that contain the tables used in the view
GRANT REFERENCE_USAGE ON DATABASE CUSTOMER_DB TO SHARE share_cust_data;
GRANT REFERENCE_USAGE ON DATABASE ADDRESS_DB TO SHARE share_cust_data;

GRANT SELECT ON VIEW VIEW_SHR_DB.PUBLIC.CUSTOMER_INFO TO SHARE share_cust_data; -- add the view to the share with SELECT permissions

-- add data consumer to the share
ALTER SHARE share_cust_data ADD ACCOUNT = <consumer_account_name>; -- cn12345


-- [login as consumer] consume share
USE ROLE ACCOUNTADMIN;
SHOW SHARES; -- list inbound share related to account

-- db, schema and table in the share but without a db attached <DB>.PUBLIC.CUSTOMER_INFO
DESCRIBE SHARE <provider_account_name>.share_cust_data; -- pr67890.share_cust_data

-- attach a database (consumer side) via share
CREATE DATABASE SHR_CUSTOMER
FROM SHARE <provider_account_name>.share_cust_data;

DESCRIBE SHARE <provider_account_name>.share_cust_data; -- db set

SELECT * FROM SHR_CUSTOMER.PUBLIC.CUSTOMER_INFO;
