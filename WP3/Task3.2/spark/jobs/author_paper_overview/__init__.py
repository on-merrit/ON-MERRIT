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
    sdg_authors = spark.read.csv(author_filename, header=True) \
        .select(['authorid'])

    logger.info('Deduplicating authors...')
    sdg_authors_before = sdg_authors
    n_sdg_authors_before = sdg_authors_before.count()
    logger.info(f'Rows before: {n_sdg_authors_before}')

    sdg_authors = sdg_authors.drop_duplicates()
    n_after = sdg_authors.count()
    logger.info(f'Rows after: {n_after}')
    logger.info(f'Difference: {n_sdg_authors_before - n_after}')

    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    logger.info('Joining with all papers')
    all_papers_from_authors = sdg_authors \
        .join(paper_author_affil, ['authorid'], how='left')

    # make sure we have unambiguous authors here
    logger.info(f'Number of rows before deduplication: {all_papers_from_authors.count()}')

    all_papers_dedup = all_papers_from_authors \
        .select(['authorid', 'paperid']) \
        .drop_duplicates()

    logger.info(f'Number of rows after deduplication: {all_papers_dedup.count()}')

    # alternatively, we could have used
    # df.groupBy('authorid').agg(countDistinct('col_name'))
    # see https://stackoverflow.com/a/46422580/3149349
    # (also needs from pyspark.sql.functions import countDistinct)

    # count the number of papers per author
    logger.info('Counting the papers per author')
    paper_counts = all_papers_dedup \
        .groupby(all_papers_dedup.authorid).count()

    papers_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_author_paper_counts.csv")

    logger.info('Writing counts to file...')
    paper_counts. \
        write.csv(papers_filename, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
