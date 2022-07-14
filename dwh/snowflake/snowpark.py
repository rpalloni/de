# https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html

import config
from snowflake.snowpark import Session, Row
from snowflake.snowpark.functions import avg, col
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType

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

# config
session.get_current_warehouse()
session.get_current_database()
session.get_current_schema()
session.get_fully_qualified_current_schema()
session.get_current_role()

session.use_warehouse('<warehouse>')
session.use_database('<db>')
session.use_schema('<schema>')
session.use_role('<role>')


# lazy evaluation => SQL statement isn’t sent to the server for execution until an action (collect, show, count) is performed
session.sql('select current_warehouse(), current_database(), current_schema()').collect()

session.sql('''CREATE OR REPLACE TABLE product_data (
            id INT,
            parent_id INT,
            category_id INT,
            name VARCHAR,
            serial_number VARCHAR,
            key INT,
            third INT)''').collect()

session.table('product_data').schema

session.sql(''' INSERT INTO product_data VALUES
    (1, 0, 5, 'Product 1', 'prod-1', 1, 10),
    (2, 1, 5, 'Product 1A', 'prod-1-A', 1, 20),
    (3, 1, 5, 'Product 1B', 'prod-1-B', 1, 30),
    (4, 0, 10, 'Product 2', 'prod-2', 2, 40),
    (5, 4, 10, 'Product 2A', 'prod-2-A', 2, 50),
    (6, 4, 10, 'Product 2B', 'prod-2-B', 2, 60),
    (7, 0, 20, 'Product 3', 'prod-3', 3, 70),
    (8, 7, 20, 'Product 3A', 'prod-3-A', 3, 80),
    (9, 7, 20, 'Product 3B', 'prod-3-B', 3, 90),
    (10, 0, 50, 'Product 4', 'prod-4', 4, 100),
    (11, 10, 50, 'Product 4A', 'prod-4-A', 4, 100),
    (12, 10, 50, 'Product 4B', 'prod-4-B', 4, 100)''').collect()

session.sql('SELECT count(*) FROM product_data').collect()
session.table('product_data').count()
session.table('product_data').show()
session.table('product_data').to_pandas()

# create snowpark DataFrame
df_table = session.table('product_data')
df_table.show() # DataFrame is lazily evaluated

df_query = session.sql('SELECT * FROM product_data')
df_query.show()

df_row = session.create_dataframe([
    Row(first_name='Alan', last_name='Terry', age=23, job='UX designer'),
    Row(first_name='Mark', last_name='Lenders', age=32, job='Web developer'),
    Row(first_name='John', last_name='Deep', age=25, job='Software engineer'),
])
df_row.show()

df_row_short = session.create_dataframe(
    [
        ['Alan', 'Terry', 23, 'UX designer'],
        ['Mark', 'Lenders', 32, 'Web developer'],
        ['John', 'Deep', 25, 'Software engineer'],
    ],
    schema=['first_name', 'last_name', 'age', 'job']
)
df_row_short.show()

schema = StructType([
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('age', IntegerType()),
    StructField('job', StringType()),
])
df_schema = session.create_dataframe([
    ['Alan', 'Terry', 23, 'UX designer'],
    ['Mark', 'Lenders', 32, 'Web developer'],
    ['John', 'Deep', 25, 'Software engineer'],
], schema)
df_schema.show()


# query
(
    session.table('product_data')
    .select(col('id'), col('name'), col('serial_number'))
    .show()
)

df_prod = session.table('product_data')
df_prod.select(df_prod['id'], df_prod['name']).show()
df_prod.select(df_prod.id, df_prod.name).show()
df_prod.filter(df_prod.id == 1).select('serial_number', 'name').show()

(
    session.table('product_data')
    .select(col('name'), col('serial_number')) # error: order matters in chaining method calls
    .filter(col('id') == 1)
    .show()
)

# join
df_emp = session.create_dataframe(
    [
        [1, 'Alan', 'Terry', 23, 'UX designer', 'DES'],
        [2, 'Mark', 'Lenders', 32, 'Web developer', 'DEV'],
        [3, 'John', 'Deep', 25, 'Software engineer', 'DEV'],
        [4, 'Enry', 'Hivy', 27, 'Budget manager', 'FIN'],
    ],
    schema=['id', 'first_name', 'last_name', 'age', 'job', 'cod']
)

df_dep = session.create_dataframe(
    [
        [1, 'DES', 'Design'],
        [2, 'DEV', 'Development'],
        [3, 'SLS', 'Sales'],
    ],
    schema=['id', 'cod', 'name']
)

(
    df_emp.join(df_dep,
                df_emp.cod == df_dep.cod, # use ['cod'] since col name is equal
                join_type='inner') # inner, left, right, full
    .select(df_emp.first_name, df_emp.last_name, df_emp.age, df_dep.cod.as_('cod'), df_dep.name)
    .to_pandas()
)

from copy import copy
from snowflake.snowpark.exceptions import SnowparkJoinException
df = session.table('product_data')

try:
    df_joined = df.join(df, col('id') == col('parent_id')) # fails as id and parent_id are in the left and right DataFrames
except SnowparkJoinException as e:
    print(e.message) # create a copy!

df_lf = session.table('product_data')
df_rg = copy(df_lf)
df_lf.join(df_rg, df_lf.id == df_rg.parent_id).count()

# view
df.create_or_replace_view(f'{session.get_current_database()}.{session.get_current_schema()}.product_view')
df.create_or_replace_temp_view(f'{session.get_current_database()}.{session.get_current_schema()}.product_temp_view')

# stage
session.sql('LIST @S3_STAGE_EMP').show()

schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('age', IntegerType()),
])
df_reader = session.read.schema(schema)
df = df_reader.csv('@S3_STAGE_EMP')
df.show()


# save DataFrame to table
(
    df_row.write
    .mode('overwrite') # append
    .save_as_table('employees')
)

session.close()
