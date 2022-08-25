import config
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf, sproc

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

# stored procedure
# CREATE OR REPLACE STAGE pystage;
# CREATE TABLE product_data_shlw_copy LIKE product_data; -- only structure

@sproc(
    session=session,
    packages=['snowflake-snowpark-python'],
    is_permanent=True,
    stage_location='@pystage',
    name='pyproc')
def run(session: Session, from_table: str, to_table: str, cnt: int) -> str:
    (
      session.table(from_table).limit(cnt)
      .write.mode('overwrite')
      .save_as_table(to_table)
    )
    return 'OK!'

session.call('pyproc', 'product_data', 'product_data_shlw_copy', 3)

# SELECT * FROM product_data_shlw_copy;
