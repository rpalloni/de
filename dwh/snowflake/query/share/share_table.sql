/* share table with another snowflake account */

CREATE DATABASE DATA_PROVIDER_DB;

CREATE TABLE TRANSACTIONS (
  TXN_ID STRING,
  TXN_DATE DATE,
  CUSTOMER_ID STRING,
  QUANTITY DECIMAL(20),
  PRICE DECIMAL(30,2),
  COUNTRY_CD STRING
);

INSERT INTO TRANSACTIONS
SELECT
    UUID_STRING() AS TXN_ID,
    DATEADD(DAY,UNIFORM(1, 500, RANDOM()) * -1, '2022-07-15') AS TXN_DATE,
    UUID_STRING() AS CUSTOMER_ID,
    UNIFORM(1, 10, RANDOM()) AS QUANTITY,
    UNIFORM(1, 200, RANDOM()) AS PRICE,
    RANDSTR(2,RANDOM()) AS COUNTRY_CD
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- create share object as admin
USE ROLE ACCOUNTADMIN;
CREATE SHARE share_trx_data;

-- grant usage on the database and the schema to subsequently provide access to the table
GRANT USAGE ON DATABASE DATA_PROVIDER_DB TO SHARE share_trx_data;
GRANT USAGE ON SCHEMA DATA_PROVIDER_DB.PUBLIC TO SHARE share_trx_data;
GRANT SELECT ON TABLE  DATA_PROVIDER_DB.PUBLIC.TRANSACTIONS TO SHARE share_trx_data; -- add the table to the share with SELECT permissions

-- add data consumer to the share
ALTER SHARE share_trx_data ADD ACCOUNT = <consumer_account_name>; -- cn12345


-- [login as consumer] consume share
USE ROLE ACCOUNTADMIN;
SHOW SHARES; -- list inbound share related to account

-- db, schema and table in the share but without a db attached <DB>.PUBLIC.TRANSACTIONS
DESCRIBE SHARE <provider_account_name>.share_trx_data; -- pr67890.share_trx_data

-- attach a database (consumer side) via share
CREATE DATABASE SHR_TRANSACTIONS
FROM SHARE <provider_account_name>.share_trx_data;

DESCRIBE SHARE <provider_account_name>.share_trx_data; -- db set

SELECT * FROM SHR_TRANSACTIONS.PUBLIC.TRANSACTIONS;
