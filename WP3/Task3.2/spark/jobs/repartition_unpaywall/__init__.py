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
    logger.info('Injesting unpaywall data dump')

    # avoid nazis
    spark = ss

    logger.info('Reading the beast.')
    path = "/project/core/unpaywall/unpaywall_single.parquet"
    unpaywall = spark.read.parquet(path)

    logger.info('Print the schema')
    unpaywall.printSchema()

    logger.info('Repartitioning.')
    unpaywall = unpaywall.repartition(300)

    logger.info('Write to disk.')
    unpaywall.write.parquet("/project/core/unpaywall/unpaywall.parquet")

    logger.info('Done.')
