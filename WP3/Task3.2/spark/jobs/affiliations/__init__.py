"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from os.path import join


def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Starting to merge files ...')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    affiliations = ss.table(db_name + '.affiliations')
    gridinfo = ss.table(db_name + '.gridinfo')

    logger.info("Searching for know center")
    graz = affiliations.filter(affiliations.displayname.contains('Graz'))

    graz_full = graz.join(gridinfo, ['gridid'], how='left')

    logger.info('Writing to file...')
    # save the data for the current country
    output_filename = join(cfg['hdfs']['onmerrit_dir'], "affiliation_sample.csv")

    graz_full. \
        write.csv(output_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    logger.info('Done.')
