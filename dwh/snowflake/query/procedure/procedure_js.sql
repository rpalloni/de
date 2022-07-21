/* combine multiple SQL statements in a transaction and commit/rollback the whole block */

USE DATABASE COOKBOOK;

CREATE OR REPLACE TABLE credit (
	account string,
	amount int,
	payment_ts timestamp
);

CREATE OR REPLACE TABLE debit (
	account string,
	amount int,
	payment_ts timestamp
);

CREATE OR REPLACE PROCEDURE sp_credit_debit(ACCOUNT_NUMBER string, ACCOUNT_AMOUNT float, INDUCE_FAULT boolean)
  RETURNS string
  LANGUAGE javascript
AS
$$
var sql_command_debit = "insert into DEBIT values ('"+ ACCOUNT_NUMBER +"', "+ ACCOUNT_AMOUNT+ ", current_timestamp());";
var sql_command_credit = "insert into CREDIT select * from DEBIT where payment_ts > (select IFF(max(payment_ts) IS NULL, to_date('1970-01-01'), max(payment_ts)) from CREDIT);";

snowflake.execute({sqlText: "BEGIN TRANSACTION;"});
/* A transaction is a sequence of SQL statements that are processed as an atomic unit.
All statements in the transaction are either applied (i.e. committed) or undone (i.e. rolled back) together. */
try {
	snowflake.execute({sqlText: sql_command_debit});
	if(INDUCE_FAULT){
		snowflake.execute({sqlText: "DELETE FROM TABLEXYZ;"});
	}
	snowflake.execute({sqlText: sql_command_credit});
	snowflake.execute({sqlText: "COMMIT;"});
	return "Transaction OK";
} catch (e) {
	snowflake.execute({sqlText: "ROLLBACK;"});
	return "Transaction KO - " + e;
}
$$;


ALTER SESSION SET AUTOCOMMIT = FALSE; -- manage commit in the transaction
ALTER SESSION SET TIMEZONE = 'Europe/Rome'; -- set timezone


CALL sp_credit_debit('BK001',50,FALSE); -- call procedure

SELECT * FROM debit;
SELECT * FROM credit;

CALL sp_credit_debit('BK002',250,TRUE); -- FAIL: sql_command_debit execution before error is rolled back

SELECT * FROM debit;
