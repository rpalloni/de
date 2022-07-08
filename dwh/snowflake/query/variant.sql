CREATE DATABASE CREDITCARDS;

USE DATABASE CREDITCARDS;

-- create external stage pointing to the private bucket with JSON file
/*
{
	"data_set":"credit_cards",
	"extract_date":"2019-10-26",
	"credit_cards": [
		{
			"CreditCardNo": "527147 8139869068",
			"CreditCardHolder": "Alfreda Y. Meyers",
			"CardPin": "1634",
			"CardCVV": "929",
			"CardExpiry": "03/20"
		},
*/

CREATE OR REPLACE STAGE JSON_STAGE 
STORAGE_INTEGRATION = S3_INTEGRATION -- defined in ddl
URL = 's3://abc123/cc_data.json'
FILE_FORMAT = (TYPE = JSON);

LIST @JSON_STAGE;
 
-- load data in a column with 1 record
SELECT  PARSE_JSON($1)
FROM @JSON_STAGE;
 
-- load the JSON data in table with one VARIANT column
CREATE TABLE CREDIT_CARD_TEMP (
    JSON_DATA VARIANT
);

-- copy the JSON data into the table
COPY INTO CREDIT_CARD_TEMP FROM @JSON_STAGE;

SELECT JSON_DATA FROM CREDIT_CARD_TEMP;

-- parse JSON fields
SELECT 
JSON_DATA:data_set,
JSON_DATA:extract_date,
JSON_DATA:credit_cards 
FROM CREDIT_CARD_TEMP;

-- access nested fields
SELECT 
JSON_DATA:credit_cards[0].CreditCardNo,
JSON_DATA:credit_cards[0].CreditCardHolder 
FROM CREDIT_CARD_TEMP;

-- use FLATTEN function to conver JSON into relational format
SELECT
    JSON_DATA:extract_date::date AS data_ext,
    value:CreditCardNo::string AS cc_number,
    value:CreditCardHolder::string AS cc_holder,
    value:CardPin::integer AS cc_pin,
    value:CardCVV::string AS cc_cvv,
    value:CardExpiry::string AS cc_exp
FROM
   CREDIT_CARD_TEMP, 
   lateral flatten( input => JSON_DATA:credit_cards );

  
-- \n delimited JSON
/*
{  "CreditCardNo": "36526505572729",  "CreditCardHolder": "Price M. Cochran",  "CardPin": "6609",  "CardCVV": "979",  "CardExpiry": "10/2020"}
{  "CreditCardNo": "646 22682 40124 108",  "CreditCardHolder": "Daquan I. Sullivan",  "CardPin": "6954",  "CardCVV": "197",  "CardExpiry": "09/2019"}
 */
CREATE OR REPLACE STAGE NJSON_STAGE 
STORAGE_INTEGRATION = S3_INTEGRATION -- defined in ddl
URL = 's3://abc123/cc_njdata.json'
FILE_FORMAT = (TYPE = JSON, STRIP_OUTER_ARRAY = TRUE);

LIST @NJSON_STAGE;
 
-- load data in a column with n records
SELECT  PARSE_JSON($1)
FROM @NJSON_STAGE;


-- parse and convert the JSON into relational format
SELECT  
PARSE_JSON($1):CreditCardNo::string AS CreditCardNo,
PARSE_JSON($1):CreditCardHolder::string AS CreditCardHolder,
PARSE_JSON($1):CardPin::integer AS CardPin,
PARSE_JSON($1):CardExpiry::string AS CardExpiry,
PARSE_JSON($1):CardCVV::string AS CardCVV
FROM @NJSON_STAGE;
 
-- create a new table with the JSON data
CREATE TABLE CREDIT_CARD_DATA AS
SELECT  
PARSE_JSON($1):CreditCardNo::string AS CreditCardNo,
PARSE_JSON($1):CreditCardHolder::string AS CreditCardHolder,
PARSE_JSON($1):CardPin::integer AS CardPin,
PARSE_JSON($1):CardExpiry::string AS CardExpiry,
PARSE_JSON($1):CardCVV::string AS CardCVV
FROM @NJSON_STAGE;

-- validate data inserted successfully
SELECT * FROM CREDIT_CARD_DATA;

