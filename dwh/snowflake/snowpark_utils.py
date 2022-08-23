import config
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf

connection_parameters = {
    'account': config.snowflake_account,
    'user': config.snowflake_user,
    'password': config.snowflake_password,
    'role': config.snowflake_role,
    'warehouse': config.snowflake_warehouse,
    'database': config.snowflake_database,
    'schema': config.snowflake_schema
}

session = (
    Session.builder
    .configs(connection_parameters)
    .create()
)

with session.query_history() as history:
    df = session.table('product_data')
history.queries # empty: no SQL evaluation yet

df.explain()

with session.query_history() as history:
    df.show()
history.queries

with session.query_history() as history:
    df.count()
history.queries

with session.query_history() as history:
    tquery = (
        df.filter(df.id == 1)
        .select('serial_number', 'name')
        .show()
    )

history.queries[0].sql_text

df.to_pandas()

# udf
@udf(session=session)
def is_main_prod(serial: str) -> bool:
    '''find main product based on serial len'''
    if len(serial) == 6:
        return True
    else:
        return False

df.select(is_main_prod(col('serial_number'))).show()
