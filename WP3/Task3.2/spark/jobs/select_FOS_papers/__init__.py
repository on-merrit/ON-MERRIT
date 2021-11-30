"""
This script samples papers from all of MAG.

Inclusion criteria:
- must have a DOI
- publication year is 2008-2018
- doctype is either journal or conference

We sample 10mio papers for now, and keep track of the FOS.

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
    logger.info('Finding all papers')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # avoid nazis
    spark = ss

    papers_df = spark \
        .table(db_name + '.papers') \
        .drop_duplicates()
    paper_field_of_study = spark \
        .table(db_name + '.paperfieldsofstudy')
    field_of_study = spark \
        .table(db_name + '.fieldsofstudy') \
        .select(['fieldofstudyid', 'displayname', 'normalizedname', 'level']) \
        .withColumnRenamed('displayname', 'fos_displayname') \
        .withColumnRenamed('normalizedname', 'fos_normalizedname')
    # ref for renaming cols: https://stackoverflow.com/a/36302241/3149349

    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    # only keep journal and conference papers
    papers_only = papers_df.where(f.col("doctype").isin(["Journal", "Conference"]))

    # drop those without a DOI
    with_doi = papers_only \
        .filter(papers_only.doi.isNotNull())

    # restrict date range
    correct_dates = with_doi \
        .filter((with_doi.year >= 2008) & (with_doi.year <= 2018))

    # sample a fraction (from https://stackoverflow.com/a/65256780/3149349)
    # get total count
    N = correct_dates.count()
    logger.info("Selected total sample, which contains this number of papers:\n")
    logger.info(N)

    target_fraction = 1e+7/N

    selected_sample = correct_dates.sample(False, target_fraction, seed=20211130)

    n = selected_sample.count()
    logger.info("Selected random sample, which contains this number of papers:\n")
    logger.info(n)

    ##  select columns ---------------------------
    # available columns:
    # "paperid","fieldofstudyid","fos_displayname","fos_normalizedname","score","rank","doi","doctype","papertitle",
    # "originaltitle","booktitle","year","date","publisher","journalid","conferenceseriesid","conferenceinstanceid",
    # "volume","issue","firstpage","lastpage","referencecount","citationcount","estimatedcitation","originalvenue","familyid","createddate"

    logger.info('Selecting columns')
    sample_with_cols = selected_sample \
        .select(
        "paperid",
        "doi", "doctype", "papertitle", "originaltitle", "booktitle", "year",
        "date", "publisher", "journalid", "conferenceinstanceid",
        "referencecount", "citationcount", "originalvenue", "familyid"
    )


    # write papers to file
    paper_filename = path.join(cfg['hdfs']['onmerrit_dir'], "fos_papers.csv")

    logger.info('Writing papers to file...')
    sample_with_cols. \
        write.csv(paper_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    # Find all authors of the papers
    sdg_author_affils = sample_with_cols \
        .select(['paperid']) \
        .join(paper_author_affil, ['paperid'], how='left')

    # write authors to file
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "fos_author_paper_affil.csv")

    logger.info('Writing authors to file...')
    sdg_author_affils. \
        write.csv(author_filename, mode="overwrite", header=True,
                  sep=",", quoteAll=True)

    # TODO: write paper fos mapping for our papers to file

    logger.info('Done.')
