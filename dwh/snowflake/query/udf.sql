USE DATABASE COOKBOOK;

-- scalar UDF
CREATE FUNCTION profit_after_tax(cost float, revenue float)
RETURNS float
AS
$$
  (revenue - cost) * (1 - 0.1) -- 10% tax
$$;

SELECT profit_after_tax(100,120); -- call function

SELECT
DD.D_DATE,
SUM(profit_after_tax(SS_WHOLESALE_COST,SS_SALES_PRICE)) AS real_profit
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.STORE_SALES SS
INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL.DATE_DIM DD
ON SS.SS_SOLD_DATE_SK = DD.D_DATE_SK
WHERE DD.D_DATE BETWEEN '2003-01-01' AND '2003-12-31'
AND profit_after_tax(SS_WHOLESALE_COST,SS_SALES_PRICE) < -50 -- detect loss
GROUP BY DD.D_DATE;


-- table UDF
CREATE FUNCTION CustomerOrders()
RETURNS TABLE(CustomerName String, TotalSpent Number(12,2))
as
$$
    SELECT C.C_NAME AS CustomerName, SUM(O.O_TOTALPRICE) AS TotalSpent
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
    ON C.C_CUSTKEY = O.O_CUSTKEY
    GROUP BY C.C_NAME
$$;

SELECT * FROM TABLE(CustomerOrders());


CREATE FUNCTION CustomerOrders(CustomerName String) -- different signature
RETURNS TABLE(CustomerName String, TotalSpent Number(12,2))
as
$$
    SELECT C.C_NAME AS CustomerName, SUM(O.O_TOTALPRICE) AS TotalSpent
    FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS O
    INNER JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER C
    ON C.C_CUSTKEY = O.O_CUSTKEY
    WHERE C.C_NAME = CustomerName
    GROUP BY C.C_NAME
$$;

SELECT * FROM TABLE(CustomerOrders('Customer#000062993'));

-- system table functions https://docs.snowflake.com/en/sql-reference/functions-table.html#list-of-system-defined-table-functions
SELECT seq1()
FROM TABLE(generator(rowCount => 5));

SELECT *
FROM TABLE(information_schema.query_history_by_session())
ORDER BY start_time;
