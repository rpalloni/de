import pytest
import pandas as pd
from pyspark.sql import SparkSession
from jobs.job1 import _transform_data

# pytest test_app.py

@pytest.fixture(scope='session')
def spark_session(request):
    '''instantiate SparkSession once and pass it to every test as a parameter
    fixtures configure the test environment and clean up after the tests'''
    spark_session = (
        SparkSession.builder
        .master('local[*]')
        .appName('test')
        .getOrCreate()
    )
    # release the resources allocated by the fixture
    request.addfinalizer(lambda: spark_session.sparkContext.stop())

    return spark_session


class TestJobs:
    def test_transform_data(self, spark_session):

        test_data = spark_session.createDataFrame(
            [(1, 'Toy Story (1995)', 'Adventure'), (160646, 'Goat (2016)', 'Drama')],
            ['movieId', 'title', 'genres']
        )

        expected_data = spark_session.createDataFrame(
            [(1, 'Toy Story ', '1995'), (160646, 'Goat ', '2016')],
            ['movieId', 'title', 'year']
        ).toPandas()

        real_data = _transform_data(test_data).toPandas()

        pd.testing.assert_frame_equal(real_data, expected_data, check_dtype=False)
