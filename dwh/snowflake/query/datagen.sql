-- generate data https://docs.snowflake.com/en/sql-reference/functions-data-generation.html

/*				    SEQ1()		|		  SEQ2()	  |	  	SEQ4()	  |	 	  SEQ8()
signed		-128 to 127		|	~ -32k to 32k	|	~ -2bn to 2bn	|	~ -9qn to 9qn (quintillion 10^18)
unsigend	  0 to 255		|	~   0 to 65k	|	~   0 to 4bn	|	~   0 to 18qn
*/

SELECT seq2()
FROM table(generator(rowCount => 5));
-- FROM TABLE(<function>) => table function [table output]

SELECT uniform(1,100,random(123)) FROM table(generator(rowCount => 5));
SELECT uniform(1,500,random())*-1 as neg_int FROM table(generator(rowCount => 5));

SELECT uuid_string() FROM table(generator(rowCount => 5));

SELECT dateadd(DAY, 1, '2022-07-15') AS add_day;
SELECT dateadd(DAY, '+' || seq4(), current_date()) as ten_days_after FROM TABLE (generator(rowcount=>10));
SELECT dateadd(DAY, '-' || seq4(), current_date()) as ten_days_before FROM TABLE (generator(rowcount=>10));
SELECT dateadd(DAY, uniform(1,500,random())*-1, '2022-07-15') as rnd_days_before FROM table(generator(rowCount => 5));

SELECT upper(randstr(2,random())) AS country_code FROM table(generator(rowCount => 5));

-- recursive sequences
SELECT mod(seq4(), 5) AS remainder, seq4() AS dividend, 5 AS divisor
FROM TABLE (generator(rowcount=>10));

SELECT mod(uniform(1, 100, random(123)),5) AS rm, uniform(1, 100, random(123)) AS dd, 5 AS ds
FROM TABLE (generator(rowcount=>10));

SELECT COALESCE(NULL, NULL, 0) -- return first non null in a sequence

-- pk
CREATE SEQUENCE seq_users
START WITH = 1000
INCREMENT BY = 5;

CREATE TABLE users (
  user_id INT DEFAULT seq_users.nextval,
  user_name STRING,
  user_age INT
)

INSERT INTO users(user_name, user_age) values ('abcd1234', 28);
INSERT INTO users(user_name, user_age) values ('fgjk5678', 32); -- pk auto-increment

-- time sequences
SELECT (to_date('2022-01-01') + seq4()) cal_date,
DATE_PART(day, cal_date) as cal_day,
DATE_PART(month, cal_date) as cal_month,
DATE_PART(year, cal_date) as cal_year,
DECODE(cal_month,
   1, 'January',
   2, 'February',
   3, 'March',
   4, 'April',
   5, 'May',
   6, 'June',
   7, 'July',
   8, 'August',
   9, 'September',
   10, 'October',
   11, 'November',
   12, 'December') as cal_month_name,
DATE_TRUNC('month', cal_date) as cal_first_day,
DATEADD('day', -1, DATEADD('month', 1, DATE_TRUNC('month', cal_date))) as cal_last_day,
DATEADD('day', -1, DATEADD('month', 3, DATE_TRUNC('quarter', cal_date))) as cal_qrt_end
FROM TABLE(GENERATOR(ROWCOUNT => 365))
--ORDER BY cal_dt desc;
