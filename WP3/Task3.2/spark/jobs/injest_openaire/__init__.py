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

    logger.info('Done.')
