/* system data and objects */
USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;

SELECT * FROM DATABASES;
SELECT * FROM SESSIONS;
SELECT * FROM ROLES;
SELECT * FROM USERS;
SELECT * FROM FILE_FORMATS;
SELECT * FROM QUERY_HISTORY ORDER BY EXECUTION_TIME DESC;

SELECT 	
	TABLE_CATALOG, -- db
	TABLE_NAME,
	ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED, -- convert to GBs
	TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE,
	FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE
FROM TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED DESC;


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

ALTER SESSION SET TIMEZONE = 'Europe/Rome';

SELECT 
localtime(), 
localtimestamp(), 
current_time(), 
current_timestamp(),
sysdate(); -- current timestamp for the system, but in the UTC time zone;




