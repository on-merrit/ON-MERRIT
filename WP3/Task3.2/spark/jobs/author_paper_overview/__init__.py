"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
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
    logger.info('Count papers per author.')

    # avoid nazis
    spark = ss

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    logger.info('Reading the tables')
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_authors.csv")
    sdg_authors = spark.read.csv(author_filename)

    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    logger.info('Joining with keys')
    sdg_paper_author = sdg_authors \
        .join(paper_author_affil, ['paperid'], how='left')

    logger.info('Joining with all papers')
    all_papers_from_authors = sdg_paper_author \
        .join(paper_author_affil, ['authorid'], how='left')

    logger.info('Counting the papers per author')
    paper_counts = all_papers_from_authors \
        .groubpy(all_papers_from_authors.authorid).count()

    papers_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_author_paper_counts.csv")

    logger.info('Writing counts to file...')
    paper_counts. \
        write.csv(papers_filename, mode="error", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
