"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from os.path import join
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
    logger.info('Starting to merge files ...')

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

    # select only climate, health and agriculture
    sdg_fields = ["Climatology", "Agricultural science", "Medicine"]
    field_of_study_selection = field_of_study\
        .where(col("displayname").isin(sdg_fields))  # https://stackoverflow.com/questions/35870760/filtering-a-pyspark-dataframe-with-sql-like-in-clause

    sdg_paper_ids = field_of_study_selection \
        .join(paper_field_of_study, ['fieldofstudyid'], how='left')

    sdg_papers = sdg_paper_ids.join(papers_df, ['paperid'], how='left')

    # sampling fractions
    # health has 29860000 papers -> 0.000168 percent
    # for agriculture 0.02
    # for climate 0.14
    fractions = {
        'medicine': 0.000168,
        'agricultural science': 0.02,
        'climatology': 0.014,
    }

    logger.info("Sampling from SDG disciplines...")
    stratified_sample = sdg_papers.sampleBy('normalizedname', fractions,
                                            seed=0)

    logger.info('Writing to file...')
    # save the data for the current country
    output_filename = join(cfg['hdfs']['onmerrit_dir'], "sdg_sample.csv")

    stratified_sample. \
        write.csv(output_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    logger.info('Done.')
