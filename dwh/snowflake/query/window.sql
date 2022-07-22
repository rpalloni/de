/* window function operates on a group (window) of related rows */

USE DATABASE COOKBOOK;

CREATE OR REPLACE TABLE bank_account AS
SELECT
	mod(seq4(), 5) AS customer_id,
	(mod(uniform(1, 100, random()), 5)+1)*100 AS deposit,
	dateadd(DAY, '-' || seq4(), current_date()) AS deposit_dt
FROM TABLE (generator(rowcount=>20));

-- deposit larger than the sum of last two
SELECT
	customer_id,
	deposit_dt,
	deposit,
	deposit > COALESCE(sum(deposit) OVER (PARTITION BY customer_id ORDER BY deposit_dt  ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 0) AS high_deposit_alert
	-- add a final 0 to coalesce lists for cases when there are no two preceding deposits
FROM bank_account
ORDER BY customer_id, deposit_dt DESC

-- avg deposit in window and compare with current deposit
SELECT
	customer_id,
	deposit_dt,
	deposit,
	COALESCE(avg(deposit) OVER (PARTITION BY customer_id ORDER BY deposit_dt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS past_avg_deposit,
	deposit > past_avg_deposit AS high_deposit_alert
FROM bank_account
ORDER BY customer_id, deposit_dt DESC
