/* clusters and micro-parititons 
Snowflake automatically partitions tables. However,data pertaining to a certain filter value 
are distributed across ALL partitions and a full table scan is required.
Re-clustering the table redistributes the data such that the data for a given filter are combined into
FEW partitions speeding up the scanning process and improving the query performance.

https://docs.snowflake.com/en/user-guide/tables-micro-partitions.html */

ALTER SESSION SET USE_CACHED_RESULT=FALSE; -- cache off for speed check

CREATE DATABASE CLUSTERING;

CREATE OR REPLACE TABLE sensor_data (
	CREATE_TS BIGINT,
	SENSOR_ID BIGINT,
	SENSOR_READING FLOAT
);


-- generate data https://docs.snowflake.com/en/sql-reference/functions-data-generation.html

SELECT seq8() FROM table(generator(rowCount => 5));

SELECT uniform(1,100,random(1235)) FROM table(generator(rowCount => 5));

/*				SEQ1()		|		SEQ2()		|		SEQ4()		|	 	SEQ8()	
signed		-128 to 127		|	~ -32k to 32k	|	~ -2bn to 2bn	|	~ -9qn to 9qn (quintillion 10^18)
unsigend	  0 to 255		|	~   0 to 65k	|	~   0 to 4bn	|	~   0 to 18qn 
*/

SELECT uuid_string() FROM table(generator(rowCount => 5));

SELECT dateadd(DAY, 1, '2022-07-15') AS add_day;
SELECT uniform(1,500,random())*-1 neg_int FROM table(generator(rowCount => 5));
SELECT dateadd(DAY, uniform(1,500,random())*-1, '2022-07-15') days_before FROM table(generator(rowCount => 5));

SELECT upper(randstr(2,random())) AS country_code FROM table(generator(rowCount => 5));


-- mocking data
INSERT INTO sensor_data (
	SELECT 
	(SEQ4())::BIGINT AS CREATE_TS,
	uniform(1,999999,RANDOM(123))::BIGINT SENSOR_ID,
	uniform(0::float, 1::float,RANDOM(456))::float SENSOR_READING
	FROM TABLE(generator(rowCount => 100000000))
	ORDER BY CREATE_TS
);

ALTER TABLE sensor_data CLUSTER BY (CREATE_TS); -- clustering: CLUSTER BY (col1, col2)


SELECT count(*) CNT, AVG(SENSOR_READING) MEAN
FROM sensor_data 
WHERE CREATE_TS BETWEEN 100000 AND 199999; -- 132 ms execution

SELECT count(*) CNT, AVG(SENSOR_READING) MEAN
FROM sensor_data 
WHERE SENSOR_ID BETWEEN 10000 AND 10999; -- 468 ms execution


CREATE OR REPLACE MATERIALIZED VIEW mv_sensor_data (
	CREATE_TS,
	SENSOR_ID,
	SENSOR_READING
) CLUSTER BY (SENSOR_ID) AS  -- clustering
SELECT CREATE_TS, SENSOR_ID, SENSOR_READING
FROM sensor_data;

SELECT count(*) CNT, AVG(SENSOR_READING) MEAN
FROM mv_sensor_data 
WHERE SENSOR_ID BETWEEN 10000 AND 10999; -- 155 ms execution


SELECT TABLE_NAME, CLUSTERING_KEY FROM INFORMATION_SCHEMA.TABLES;
SHOW TABLES LIKE 'sensor_data'; -- cluster_by
SELECT SYSTEM$CLUSTERING_INFORMATION('sensor_data', '(create_ts)');



CREATE OR REPLACE TABLE TRANSACTIONS ( 
	txn_id STRING,
	txn_date DATE,
	customer_id STRING,
	quantity INTEGER,
	price DECIMAL(10,2),
	country_cd STRING
);

INSERT INTO TRANSACTIONS (
	SELECT 
	uuid_string() AS txn_id,
	dateadd(DAY, uniform(1,500,random()) * -1, '2022-07-15') AS txn_date,
	uuid_string() AS customer_id,
	uniform(1,100,random()) AS quantity,
	uniform(1,1000, random()) AS price,
	upper(randstr(2,random())) AS country_cd
	FROM TABLE(generator(rowCount => 100000000))
);


SELECT top 5 * FROM TRANSACTIONS;

-- without cluster
SELECT count(*) 
FROM TRANSACTIONS 
WHERE TXN_DATE BETWEEN dateadd(DAY, -31, '2022-07-15') AND '2022-07-15';


USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;

SELECT 
query_id, 
query_text, 
bytes_scanned, 
partitions_scanned, -- 248
partitions_total 	-- 248
FROM QUERY_HISTORY 	-- [NOTE: latency in history update]
WHERE start_time > dateadd(MINUTE, -30, current_timestamp())
AND query_text LIKE '%TRANSACTIONS%'
ORDER BY start_time DESC;

USE DATABASE SENSORS;
-- cluster by FILTER 
ALTER TABLE TRANSACTIONS CLUSTER BY (TXN_DATE); -- [NOTE: latency in re-clustering]

-- with cluster
SELECT count(*) 
FROM TRANSACTIONS 
WHERE TXN_DATE BETWEEN dateadd(DAY, -31, '2022-07-15') AND '2022-07-15';


USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;

SELECT 
query_id, 
query_text, 
bytes_scanned, 
partitions_scanned, -- 16
partitions_total 	-- 237
FROM QUERY_HISTORY 
WHERE start_time > dateadd(MINUTE, -30, current_timestamp())
AND query_text LIKE '%TRANSACTIONS%'
ORDER BY start_time DESC;


