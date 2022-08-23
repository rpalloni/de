-- dbt build this model (CTE) in DWH by wrapping it in a CREATE VIEW AS or CREATE TABLE AS statement
-- file name is assiged to the materialized object (view)

WITH raw_reviews AS (
    -- SELECT * FROM AIRBNB.RAW.RAW_REVIEWS
    SELECT * FROM {{source('airbnb', 'reviews')}}
)
SELECT
    listing_id,
    date AS review_date,
    reviewer_name,
    comments AS review_text,
    sentiment AS review_sentiment
FROM
    raw_reviews