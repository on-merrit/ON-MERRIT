"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from os import path
from pyspark.sql.functions import col


def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Finding all papers')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    papers_df = ss \
        .table(db_name + '.papers') \
        .drop_duplicates()
    paper_field_of_study = ss \
        .table(db_name + '.paperfieldsofstudy')
    field_of_study = ss \
        .table(db_name + '.fieldsofstudy') \
        .select(['fieldofstudyid', 'displayname', 'normalizedname'])
    authors = ss \
        .table(db_name + '.authors') \
        .select(['authorid', 'normalizedname', 'displayname',
                 'lastknownaffiliationid'])
    paper_author_affil = ss \
        .table(db_name + '.paperauthoraffiliations')
    affiliations = ss \
        .table(db_name + '.affiliations')

    # select Climate change, Agriculture, Medicine and Virus
    sdg_fields = [132651083, 118518473, 71924100, 2522874641]

    # adapted from:
    # https://stackoverflow.com/questions/35870760/filtering-a-pyspark-dataframe-with-sql-like-in-clause
    field_of_study_selection = field_of_study\
        .where(col("fieldofstudyid").isin(sdg_fields))

    sdg_paper_ids = field_of_study_selection \
        .join(paper_field_of_study, ['fieldofstudyid'], how='left')

    sdg_papers = sdg_paper_ids.join(papers_df, ['paperid'], how='left')

    logger.info("Selected all papers.")

    # logger.info("Printing the number of papers we found:\n")
    # print out what we sampled
    # sdg_papers.groupby(sdg_papers.fieldofstudyid).count().show()

    # write papers to file
    output_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_papers.csv")

    if not path.exists(output_filename):
        logger.info('Writing papers to file...')
        sdg_papers. \
            write.csv(output_filename, mode="error", header=True,
                      sep=",", quoteAll=True)
    else:
        logger.info('Papers csv already exists.')

    # Find all authors of the papers
    refs_to_authors_and_affils = sdg_papers.join(paper_author_affil, ['paperid'],
                                                 how='left')
    sdg_authors = refs_to_authors_and_affils.join(authors, ['authorid'],
                                                  how='left')

    sdg_authors = sdg_authors.drop(*['affiliationid', 'originalaffiliation'])

    # write authors to file
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_authors.csv")

    if not path.exists(author_filename):
        logger.info('Writing authors to file...')
        sdg_authors. \
            write.csv(author_filename, mode="error", header=True,
                      sep=",", quoteAll=True)
    else:
        logger.info('Authors csv already exists.')

    logger.info('Done.')
