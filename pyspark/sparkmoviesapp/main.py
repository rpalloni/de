import json
import importlib
import argparse
from pyspark.sql import SparkSession


def _parse_arguments():
    '''parse arguments provided by spark-submit command'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--job', required=True)
    return parser.parse_args()


def main():
    '''app entry point'''
    args = _parse_arguments()

    with open('config.json', 'r') as file:
        config = json.load(file)

    spark = (
        SparkSession.builder
        .master('local[1]')
        .appName(config.get('app_name'))
        .getOrCreate()
    )

    job_module = importlib.import_module(f'jobs.{args.job}')
    job_module.run_job(spark, config)

if __name__ == '__main__':
    main()
