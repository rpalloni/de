/* dbt builds a directed acyclic graph (DAG) based on the interdependencies between models:
- nodes of the graph are models 
- edges between the nodes are defined by ref functions => a model in a ref function is recognized as a PREDECESSOR of the current model.
When dbt runs, models are executed in the order specified by the DAG (there is no need to explicitly define the order of execution) */

WITH src_listings AS (
    SELECT * FROM {{ ref('src_listings') }} -- jinja template tag for reference
)
SELECT
    listing_id,
    listing_name,
    room_type,
    CASE
        WHEN minimum_nights = 0 THEN 1
        ELSE minimum_nights
    END AS minimum_nights,
    host_id,
    REPLACE(price_str,'$') :: NUMBER(10,2) AS price,
    created_at,
    updated_at
FROM
    src_listings
