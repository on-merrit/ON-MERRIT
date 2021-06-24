"""
This script simply maps affiliations with countries and exports it as csv
"""


import sys
import logging
from os import path


def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # avoid nazis
    spark = ss

    affils = spark.table(db_name + '.affiliations')

    # how to drop a column: https://stackoverflow.com/a/32653341/3149349
    grid = spark.table(db_name + '.gridinfo').drop('name')

    affils_with_grid = affils.join(grid, "gridid", how="left")

    affils_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "affiliations_with_country.csv")

    affils_with_grid. \
        write.csv(affils_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    logger.info('Done.')
