/* dbt builds a directed acyclic graph (DAG) based on the interdependencies between models:
- nodes of the graph are models 
- edges between the nodes are defined by ref functions => a model in a ref function is recognized as a PREDECESSOR of the current model.
When dbt runs, models are executed in the order specified by the DAG (there is no need to explicitly define the order of execution) */

WITH src_hosts AS (
    SELECT * FROM {{ ref('src_hosts') }}
)
SELECT
    host_id,
    NVL(host_name,'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM
    src_hosts
