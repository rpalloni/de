from pyspark.sql.functions import col
from shared.udfs import get_movie_title_udf, get_movie_year_udf

# ETL

def _extract_data(session, config):
    '''read data from csv'''
    return (
        session.read
        .format('csv')
        .option('header', 'true')
        .load(f"{config.get('input_data_path')}/movies.csv")
    )

def _transform_data(rawdf):
    '''apply column tranformation'''
    return rawdf.select(
        col('movieId'),
        get_movie_title_udf('title').alias('title'),
        get_movie_year_udf('title').alias('year')
    )

def _load_data(config, trandf):
    '''save data to parquet'''
    (
        trandf.write
        .mode('overwrite')
        .parquet(f"{config.get('output_data_path')}/job1")
    )

def run_job(session, config):
    '''run ETL job on data'''
    _load_data(config, _transform_data(_extract_data(session, config)))
