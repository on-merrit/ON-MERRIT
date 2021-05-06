"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from os import path
import pyspark.sql.functions as f


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

    # read our set of authors
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_authors.csv")
    sdg_authors = spark.read.csv(author_filename, header=True)

    # also get the table of links between authors and papers
    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    # also get all papers
    papers_df = ss \
        .table(db_name + '.papers') \
        .select(['paperid', 'year']) \
        .drop_duplicates()

    # calculate academic age for our authors ----------------
    logger.info('Calculate year of first paper.')

    # only look at our authors
    # need to drop duplicates since we are taking all papers of the authors
    # otherwise this results in many duplicates per individual
    only_author_ids = sdg_authors.select(['authorid']).drop_duplicates()

    # find all papers from our authors
    all_paper_ids = only_author_ids \
        .join(paper_author_affil, ['authorid'], how='left')

    # find the full papers including year information
    all_papers = all_paper_ids \
        .join(papers_df, ['paperid'], how='left') \
        .select(['authorid', 'paperid', 'year'])

    # we want to find the first year (i.e. paper) per author
    # get min year per author
    first_papers = all_papers \
        .groupby('authorid') \
        .min('year') \
        .select('authorid', f.col('min(year)').alias('year_first_paper'))


    # merge the newly generated information to our table
    full_author_table = sdg_authors \
        .join(first_papers, ['author_id'], how='left')

    out_file = path.join(cfg['hdfs']['onmerrit_dir'],
                         "sdg_author_data.csv")

    logger.info('Writing author table to file...')
    full_author_table. \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
