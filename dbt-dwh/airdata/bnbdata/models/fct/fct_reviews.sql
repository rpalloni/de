-- as long as new reviews are added, dwh views are incremented
-- if source schema changes than fail the run

{{ config(
    materialized = 'incremental', 
    on_schema_change = 'fail' ) 
}}

WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)
SELECT *
FROM src_reviews
WHERE review_text IS NOT NULL -- add records with text and with date greater than the most recent in the model
{% if is_incremental() %}
AND review_date > (SELECT MAX(review_date) FROM {{ this }}) -- this [model] == fct_reviews (target table)
{% endif %}


/* New fact (review) => new record in source table

SELECT * 
FROM FCT_REVIEWS 
WHERE listing_id=3176
ORDER BY review_date DESC;

INSERT INTO RAW_REVIEWS VALUES (3176, CURRENT_TIMESTAMP(), 'Foo', 'excellent stay!', 'positive');

*/