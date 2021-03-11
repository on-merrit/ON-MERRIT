"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
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
    ## print out what we sampled
    # sdg_papers.groupby(sdg_papers.fieldofstudyid).count().show()

    logger.info('Writing to file...')
    # save the data for the current country
    output_filename = join(cfg['hdfs']['onmerrit_dir'], "sdg_papers.csv")

    stratified_sample. \
        write.csv(output_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    logger.info('Done.')
