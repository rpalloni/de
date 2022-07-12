-- Find total sales on a given calendar date.
SELECT sum(qtysold)
FROM sales, date
WHERE sales.dateid = date.dateid
AND caldate = '2008-01-05';

-- Find top 10 buyers by quantity.
SELECT firstname, lastname, total_quantity
FROM (
  SELECT buyerid, sum(qtysold) total_quantity
  FROM sales
  GROUP BY buyerid
  ORDER BY total_quantity desc limit 10) Q,
  users
WHERE Q.buyerid = userid
ORDER BY Q.total_quantity desc;


--create schema
create schema ext_file_upload;


--create table: sales
create table ext_file_upload.sales(
  salesid integer not null,
  listid integer not null distkey,
  sellerid integer not null,
  buyerid integer not null,
  eventid integer not null,
  dateid smallint not null sortkey,
  qtysold smallint not null,
  pricepaid decimal(8,2),
  commission decimal(8,2),
  saletime timestamp);

--load data from external file in s3
COPY ext_file_upload.sales from 's3://abc123/sales_tab.txt'
credentials 'aws_iam_role=arn:aws:iam::1234567891012:role/RedshiftS3Access' -- map the role to the cluster permissions
delimiter '\t'
timeformat 'MM/DD/YYYY HH:MI:SS'
region '<region>';


--validation query: get total count of sales records
SELECT count(*) from ext_file_upload.sales;

--validation query: Find total sales on a given calendar date
SELECT sum(qtysold)
FROM ext_file_upload.sales s, date
WHERE s.dateid = date.dateid
AND caldate = '2008-01-05';
