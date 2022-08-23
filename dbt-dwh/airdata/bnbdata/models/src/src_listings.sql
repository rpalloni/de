-- dbt build this model (CTE) in DWH by wrapping it in a CREATE VIEW AS or CREATE TABLE AS statement
-- file name is assiged to the materialized object (view)

WITH raw_listings AS (
    -- SELECT * FROM AIRBNB.RAW.RAW_LISTINGS
    SELECT * FROM {{source('airbnb', 'listings')}}
)
SELECT
    id AS listing_id,
    name AS listing_name,
    listing_url,
    room_type,
    minimum_nights,
    host_id,
    price AS price_str,
    created_at,
    updated_at
FROM
    raw_listings