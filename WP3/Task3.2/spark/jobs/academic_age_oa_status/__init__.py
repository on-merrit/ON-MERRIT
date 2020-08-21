"""
This script merges the datasets on author ids and OA status of papers.
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

    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():

        logger.info("\n\n\nProcessing dataset of papers from " + country_name)
        country_papers_oa_df = ss.read.csv(
            join(cfg['hdfs']['onmerrit_dir'],
                 'oa_status_'+country_name+'_papers.csv'),
            header=True, mode="DROPMALFORMED")

        country_papers_oa_df = country_papers_oa_df.select(
            'paperid', 'affiliationid', 'normalizedname', 'displayname',
            'is_OA', 'year')

        country_authors_df = ss.read.csv(
            join(cfg['hdfs']['onmerrit_dir'],
                 'author_names_' + country_name + '_papers.csv'),
            header=True, mode="DROPMALFORMED")

        country_authors_df = country_authors_df.select(
            'authorid', 'paperid', 'author_normalizedname', 'author_displayname'
        )

        country_merged = country_papers_oa_df.join(
            country_authors_df, ['paperid'], how='left')

        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "oa_and_authors_" +
                               country_name + ".csv")

        country_merged.write.csv(output_filename, mode="overwrite", header=True,
                                 sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country : " + country_name +
                    " to file " + output_filename + "\n\n")

    logger.info('Done.')
