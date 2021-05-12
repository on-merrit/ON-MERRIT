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
    logger.info('Collect all information that is related to one paper')

    # avoid nazis
    spark = ss

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    logger.info('Reading the tables')

    # read our papers
    paper_path = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_papers.csv")
    sdg_papers = spark.read.csv(paper_path, header=True)


    # read unpaywall data
    unpaywall = spark.read \
        .parquet("hdfs:///project/core/unpaywall/unpaywall.parquet")\
        .select('doi', 'is_oa', 'oa_status')


    ##  select columns ---------------------------
    # available columns:
    # "paperid","fieldofstudyid","fos_displayname","fos_normalizedname","score","rank","doi","doctype","papertitle",
    # "originaltitle","booktitle","year","date","publisher","journalid","conferenceseriesid","conferenceinstanceid",
    # "volume","issue","firstpage","lastpage","referencecount","citationcount","estimatedcitation","originalvenue","familyid","createddate"

    logger.info('Selecting columns')
    sdg_papers_selected = sdg_papers \
        .select(
        "paperid", "fieldofstudyid", "fos_displayname", "fos_normalizedname",
        "doi", "doctype", "papertitle", "originaltitle", "booktitle", "year",
        "date", "publisher", "journalid", "referencecount","citationcount",
        "originalvenue", "familyid"
    )


    # join with unpaywall
    logger.info('Joining with unpaywall')
    with_oa = sdg_papers_selected \
        .join(unpaywall, sdg_papers_selected.doi==unpaywall.doi, how='left')

    # join with funder data

    # get first author id and affil

    # get last author id and affil



    out_file = path.join(cfg['hdfs']['onmerrit_dir'],
                         "sdg_papers_collated.csv")

    logger.info('Writing paper table to file...')
    with_oa. \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
