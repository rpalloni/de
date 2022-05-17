# user defined functions
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.udf.html

from pyspark.sql.functions import udf, col, split, explode
from pyspark.sql.types import StringType, IntegerType

def _get_movie_title(title_column):
    return title_column[0:-7]

def _get_movie_year(title_column):
    return int(title_column[-5:-1])

def _get_movie_genres(title_column):
    return explode(split(col('genres'), '\\|'))

get_movie_title_udf = udf(_get_movie_title, StringType())
get_movie_year_udf = udf(_get_movie_year, IntegerType())
