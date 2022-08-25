import os
import sys
import config
import cachetools

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf, sproc, call_udf
from snowflake.snowpark.types import StructType, StructField, DecimalType, StringType
from snowflake.snowpark.types import PandasDataFrame, PandasSeries

import pandas as pd
from joblib import dump, load
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression

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

# CREATE OR REPLACE STAGE pystage STORAGE_INTEGRATION = S3_INTEGRATION URL = 's3://abc123/parquet/' FILE_FORMAT = CUSTOM_PARQUET;
# LIST @pystage;

# get data from stage
with session.query_history() as history:
    data = session.read.parquet('@pystage/ecommerce.parquet')
    dfc = data.cache_result() # load once
    dfc.show()

df = dfc.to_pandas()
df.shape
df.describe()

# train model
X = df[['AVG_SESSION_LENGTH', 'TIME_ON_APP', 'TIME_ON_WEBSITE', 'LENGTH_OF_MEMBERSHIP']]
y = df['YEARLY_AMOUNT_SPENT']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
lm = LinearRegression()
lm.fit(X_train, y_train)
coeff = pd.DataFrame(lm.coef_, X.columns, columns=['Coeff.'])
coeff

# upload model to stage
# CREATE OR REPLACE STAGE mlstore;
model_file = os.path.join('/tmp', 'model.joblib')
dump(lm, model_file)
session.file.put(model_file, '@mlstore', overwrite=True) # must be internal store

# create prediction udf
@cachetools.cached(cache={}) # load model once and not at each udf call
def load_model(filename):
    import_dir = sys._xoptions.get('snowflake_import_directory')
    if import_dir:
        return load(os.path.join(import_dir, filename))

@udf(
    is_permanent=True,
    name='predict_udf',
    stage_location='@mlstore',
    packages=['pandas', 'scikit-learn', 'cachetools'],
    imports=['@mlstore/model.joblib.gz'],
    session=session,
    replace=True
)
def run(df: PandasDataFrame[float, float, float, float]) -> PandasSeries[float]:
    return pd.Series(load_model('model.joblib.gz').predict(df))

columns = [data[col_name] for col_name in X.columns]
dfa = (
    dfc.select(
        *columns,
        call_udf('predict_udf', columns).alias('PREDICTED_SPEND'),
        col('YEARLY_AMOUNT_SPENT').alias('ACTUAL_SPEND')
    )
)

dfa.show()


# all in a stored procedure
@sproc(
    name='train_sp',
    is_permanent=True,
    stage_location='@pystage',
    packages=['snowflake-snowpark-python', 'pandas', 'scikit-learn', 'cachetools'],
    session=session,
    replace=True
)
def train(session: Session, stage: str, file_name: str, model_name: str) -> str:
    parquet_file = f'{stage}/{file_name}'
    data = session.read.parquet(parquet_file)
    df = data.to_pandas()

    X = df[['AVG_SESSION_LENGTH', 'TIME_ON_APP', 'TIME_ON_WEBSITE', 'LENGTH_OF_MEMBERSHIP']]
    y = df['YEARLY_AMOUNT_SPENT']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    lm = LinearRegression()
    lm.fit(X_train, y_train)

    model_file = os.path.join('/tmp', 'model.joblib')
    dump(lm, model_file)
    session.file.put(model_file, '@mlstore', overwrite=True) # must be internal store

    @udf(
        is_permanent=True,
        name='predict_udf',
        stage_location='@mlstore',
        packages=['pandas', 'scikit-learn', 'cachetools'],
        imports=['@mlstore/model.joblib.gz'],
        session=session,
        replace=True
    )
    def predict(df: PandasDataFrame[float, float, float, float]) -> PandasSeries[float]:
        return pd.Series(load_model('model.joblib.gz').predict(df))

    return pd.DataFrame(lm.coef_, X.columns, columns=['Coeff.']).to_json()


session.call('train_sp', '@pystage', 'ecommerce.parquet', 'moedel.joblib')

# call sproc every hour
create_task = '''
CREATE OR REPLACE TASK task_sp_ml
WAREHOUSE = COMPUTE_WH
SCHEDULE = '60 MINUTE'
COMMENT = 'call training procedure every hour'
AS
CALL train_sp('@pystage', 'ecommerce.parquet', 'moedel.joblib')
'''

session.sql(create_task).collect()
session.sql('ALTER TASK task_sp_ml RESUME').collect() # run
session.sql('ALTER TASK task_sp_ml SUSPEND').collect() # stop
