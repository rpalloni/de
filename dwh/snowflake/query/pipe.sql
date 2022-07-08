CREATE DATABASE PIPELINE;

USE DATABASE PIPELINE;

CREATE TABLE TRANSACTIONS (
	transaction_date date,
	customer_id number,
	transaction_id number,
	amount number
);

CREATE OR REPLACE STAGE PIPE_STAGE
STORAGE_INTEGRATION = S3_INTEGRATION -- defined in ddl
URL = 's3://abc123/transactions';

LIST @PIPE_STAGE;

-- create snowpipe for streaming data
CREATE OR REPLACE PIPE DATA_PIPE
AUTO_INGEST = TRUE
AS COPY INTO TRANSACTIONS FROM @PIPE_STAGE
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SHOW PIPES; -- aws ARN of notification_channel [SQS queue]

SELECT count(*) FROM TRANSACTIONS;