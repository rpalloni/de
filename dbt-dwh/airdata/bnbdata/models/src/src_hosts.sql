-- dbt build this model (CTE) in DWH by wrapping it in a CREATE VIEW AS or CREATE TABLE AS statement
-- file name is assiged to the materialized object (view)

WITH raw_hosts AS (
    -- SELECT * FROM AIRBNB.RAW.RAW_HOSTS
    SELECT * FROM {{source('airbnb', 'hosts')}}
)
SELECT
    id as host_id,
    name as host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    raw_hosts