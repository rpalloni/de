import config
from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
        user=config.snowflake_user,
        password=config.snowflake_password,
        account=config.snowflake_account,
        database=config.snowflake_database,
        schema=config.snowflake_schema,
        warehouse=config.snowflake_warehouse,
        role=config.snowflake_role
    )
)

try:
    connection = engine.connect()
    results = connection.execute('select current_warehouse(), current_database(), current_schema()').fetchall()
    print(results[0])
finally:
    connection.close()
    engine.dispose()
