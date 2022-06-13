Set up docker with PostgreSQL and (optional) notebook \
Run init.py to set up sql/json input sources

Write a reusable data processing job to compute some KPIs based on the following input sources:
1. *orders*: JSON files in data/orders.json
2. *geography*: table public.geography in local PostgreSQL instance

The job should be written to an output table in the DB and should contain, for each date and country:
1. the number of orders
2. the total value of successful orders (in EUR)
3. the average order value (in EUR)

The job should be runnable both for a single day and for all the days available

Using the produced table, find the top 3 countries with the highest orders value on June 26, 2021.
