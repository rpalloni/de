/* table setup for /dbt-dwh/dbtsnow/shop project */

create database raw;
create schema raw.jaffle_shop;
create schema raw.stripe;

-- customers
CREATE TABLE raw.jaffle_shop.customers(
	id integer,
	first_name varchar,
	last_name varchar
);

COPY INTO raw.jaffle_shop.customers (id, first_name, last_name)
FROM 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
FILE_FORMAT = (
  TYPE = 'CSV'
  field_delimiter = ','
  skip_header = 1
);

-- orders
CREATE TABLE raw.jaffle_shop.orders(
	id integer,
  	user_id integer,
  	order_date date,
  	status varchar,
  	_etl_loaded_at timestamp DEFAULT current_timestamp
);

COPY INTO raw.jaffle_shop.orders (id, user_id, order_date, status)
FROM 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
FILE_FORMAT = (
  TYPE = 'CSV'
  field_delimiter = ','
  skip_header = 1
 );
 
-- payments
CREATE TABLE raw.stripe.payment (
	id integer,
  	orderid integer,
  	paymentmethod varchar,
  	status varchar,
  	amount integer,
  	created date,
  	_batched_at timestamp DEFAULT current_timestamp
);

COPY INTO raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
FROM 's3://dbt-tutorial-public/stripe_payments.csv'
FILE_FORMAT = (
  TYPE = 'CSV'
  field_delimiter = ','
  skip_header = 1
);
  
 
 