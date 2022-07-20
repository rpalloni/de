/* system data and objects */
USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;

SELECT * FROM DATABASES;
SELECT * FROM SESSIONS;
SELECT * FROM ROLES;
SELECT * FROM USERS;
SELECT * FROM FILE_FORMATS;
SELECT * FROM QUERY_HISTORY ORDER BY EXECUTION_TIME DESC; 
SELECT query_id, bytes_scanned, partitions_scanned, partitions_total 
FROM QUERY_HISTORY 
WHERE start_time > dateadd(MINUTE, -30, current_timestamp()) -- last 30 minutes
AND query_text LIKE '%<TABLE>%'
ORDER BY start_time DESC;
/* 
WARNING: https://docs.snowflake.com/en/sql-reference/account-usage.html#data-latency
DO NOT USE: SELECT * FROM QUERY_HISTORY WHERE QUERY_ID = last_query_id()
*/

SELECT 	
	TABLE_CATALOG, -- db
	TABLE_NAME,
	ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED, -- convert to GBs
	TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE,
	FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE
FROM TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED DESC;


/* system functions */
-- https://docs.snowflake.com/en/sql-reference/functions-system.html
SELECT SYSTEM$CLUSTERING_INFORMATION('table', '(cluster_col)');


/* context utility FUNCTIONS */
select current_warehouse(), current_database(), current_schema(); 

/*	General Context			Session Context			Session Context Object
	----------------------------------------------------------------------
	current_client()		current_account()		current_warehouse()
	current_date()			current_role()			current_database()
	current_region()		current_session()		current_schema()
	current_time()			current_user()			current_schemas()
	current_timestamp()		current_statement()		invoker_role()
	current_version()		current_transaction()	invoker_share()
	localtime()				last_query_id()			is_granted_to_invoker_role()
	localtimestamp()		last_transaction()		is_role_in_session(current_session())
	sysdate()
*/

SHOW PARAMETERS LIKE '%LANGUAGE';

SHOW PARAMETERS LIKE '%TIMEZONE' IN ACCOUNT;
SHOW PARAMETERS LIKE '%TIMEZONE' IN SESSION;

ALTER SESSION SET TIMEZONE = 'Europe/Rome'; -- https://docs.snowflake.com/en/sql-reference/parameters.html#timezone

SELECT 
localtime(), 
localtimestamp(), 
current_time(), 
current_timestamp(),
sysdate(); -- current timestamp for the system, but in the UTC time zone;




