USE DATABASE snowflake_sample_data;

USE SCHEMA TPCH_SF1;

SELECT c_name, sum(l_extendedprice), sum(l_tax)
FROM CUSTOMER 
INNER JOIN ORDERS ON o_custkey = c_custkey
INNER JOIN LINEITEM ON l_orderkey = o_orderkey
GROUP BY c_name;

CREATE DATABASE reporting;
USE DATABASE reporting;

CREATE TABLE orders_report (
	report_time timestamp,
	customer_name string,
	revenue number(16,2),
	tax number(16,2)
);

CREATE TASK update_orders_report
WAREHOUSE = COMPUTE_WH
SCHEDULE = '10 MINUTE'
COMMENT = 'update orders_report table with latest data'
AS
INSERT INTO orders_report (
	SELECT 
	current_timestamp,
	c_name,
	sum(l_extendedprice),
	sum(l_tax)
	FROM snowflake_sample_data.tpch_sf1.customer
	INNER JOIN snowflake_sample_data.tpch_sf1.orders ON o_custkey = c_custkey
	INNER JOIN snowflake_sample_data.tpch_sf1.lineitem ON l_orderkey = o_orderkey
	GROUP BY current_timestamp, c_name
);

DESCRIBE TASK update_orders_report; -- created in state: suspended

-- run task
ALTER TASK update_orders_report RESUME;
DESCRIBE TASK update_orders_report; -- update state: started

SELECT name, state, completed_time, scheduled_time, error_code, error_message
FROM TABLE (information_schema.task_history());

ALTER TASK update_orders_report SUSPEND; -- stop task


-- data pipeline (tasks tree): clear data task >> insert data task
CREATE TASK clear_orders_data
WAREHOUSE = COMPUTE_WH
COMMENT = 'delete orders data in orders_report'
AS
DELETE FROM orders_report;

CREATE TASK insert_orders_data
WAREHOUSE = COMPUTE_WH
COMMENT = 'insert orders data in orders_report'
AS
INSERT INTO orders_report (
	SELECT 
	current_timestamp,
	c_name,
	sum(l_extendedprice),
	sum(l_tax)
	FROM snowflake_sample_data.tpch_sf1.customer
	INNER JOIN snowflake_sample_data.tpch_sf1.orders ON o_custkey = c_custkey
	INNER JOIN snowflake_sample_data.tpch_sf1.lineitem ON l_orderkey = o_orderkey
	GROUP BY current_timestamp, c_name
);

-- create pipeline: clear >> insert
ALTER TASK insert_orders_data ADD AFTER clear_orders_data;

DESCRIBE TASK clear_orders_data;

DESCRIBE TASK insert_orders_data; -- predecessors: clear_orders_data

-- set schedule on root task
ALTER TASK clear_orders_data SET SCHEDULE = '10 MINUTE';

-- run tasks: firts child, than root
ALTER TASK insert_orders_data RESUME;
ALTER TASK clear_orders_data RESUME;

SELECT name, state, completed_time, scheduled_time, error_code, error_message
FROM TABLE (information_schema.task_history()); -- only root task listed until execution

-- stop tasks: first root, than child
ALTER TASK clear_orders_data SUSPEND;
ALTER TASK insert_orders_data SUSPEND;


