/* analysis allows to simplify the query composition
using the dbt features (not meant to be models)  */

with mart_full_moon_reviews as (
  select * from {{ ref('mart_full_moon_reviews') }}
)
select is_full_moon, review_sentiment, count(*) as reviews
from mart_full_moon_reviews
group by is_full_moon, review_sentiment
order by is_full_moon, review_sentiment

-- dbt compile => use compiled in the DWH
