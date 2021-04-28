"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging


def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Injesting openaire funder data dump')

    # avoid nazis
    spark = ss

    logger.info('Reading the bunch')

    # https://stackoverflow.com/a/32233865/3149349
    path = "/project/core/openaire_funders/*/*.json.gz"
    funder_data = spark.read.json(path)

    logger.info('Print the schema')
    funder_data.printSchema()

    logger.info('Write to disk.')
    funder_data.write.parquet("/project/core/openaire_funders/openaire_funders.parquet")

    logger.info('Done.')
