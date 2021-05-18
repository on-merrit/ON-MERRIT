"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from os import path
import pyspark.sql.functions as f
from pyspark.sql import Window


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
        .select(['fieldofstudyid', 'displayname', 'normalizedname']) \
        .withColumnRenamed('displayname', 'fos_displayname') \
        .withColumnRenamed('normalizedname', 'fos_normalizedname')
    # ref for renaming cols: https://stackoverflow.com/a/36302241/3149349

    paper_author_affil = ss \
        .table(db_name + '.paperauthoraffiliations')

    # select Climate change, Agriculture, Medicine and Virus
    sdg_fields = [132651083, 118518473, 71924100, 2522874641]

    # adapted from:
    # https://stackoverflow.com/questions/35870760/filtering-a-pyspark-dataframe-with-sql-like-in-clause
    field_of_study_selection = field_of_study\
        .where(f.col("fieldofstudyid").isin(sdg_fields))

    sdg_paper_ids = field_of_study_selection \
        .join(paper_field_of_study, ['fieldofstudyid'], how='left')

    sdg_papers = sdg_paper_ids.join(papers_df, ['paperid'], how='left')

    # only keep papers in our window (2008-2018)
    sdg_papers = sdg_papers \
        .filter((sdg_papers.year > 2007) & (sdg_papers.year < 2019))

    logger.info("Selected all papers.")

    # logger.info("Printing the number of papers we found:\n")
    # print out what we sampled
    # sdg_papers.groupby(sdg_papers.fieldofstudyid).count().show()

    # write papers to file
    paper_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_papers.csv")

    logger.info('Writing papers to file...')
    sdg_papers. \
        write.csv(paper_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    # Find all authors of the papers
    sdg_author_affils = sdg_papers \
        .select(['paperid']) \
        .join(paper_author_affil, ['paperid'], how='left')



    # write authors to file
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_author_paper_affil.csv")

    logger.info('Writing authors to file...')
    sdg_author_affils. \
        write.csv(author_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    logger.info('Done.')
