https://github.com/dbt-labs/dbt-core/tree/main/docker

docker build --tag <your_image_name>  --target <target_name> <path/to/dockerfile>

docker build --tag dbtredshift --target dbt-redshift ./docker

docker build --tag dbtsnowflake --target dbt-snowflake ./docker


https://docs.getdbt.com/guides/getting-started/getting-set-up/setting-up-redshift

https://docs.getdbt.com/guides/getting-started/getting-set-up/setting-up-snowflake


https://docs.getdbt.com/guides/getting-started/building-your-first-project/build-your-first-models
