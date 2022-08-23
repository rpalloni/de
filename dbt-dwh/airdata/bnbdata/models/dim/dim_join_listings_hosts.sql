WITH lst AS (
    SELECT * FROM {{ ref('dim_listings_cleansed') }}
),
hst AS (
    SELECT * FROM {{ ref('dim_hosts_cleansed') }}
)
SELECT
    lst.listing_id,
    lst.listing_name,
    lst.room_type,
    lst.minimum_nights,
    lst.price,
    lst.host_id,
    hst.host_name,
    hst.is_superhost AS host_is_superhost,
    lst.created_at,
    GREATEST(lst.updated_at, hst.updated_at) AS updated_at
FROM
    lst
    LEFT JOIN hst
    ON hst.host_id = lst.host_id
