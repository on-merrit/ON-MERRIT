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

    logger.info('Reading the tables')

    # read our papers
    paper_path = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_papers.csv")
    sdg_papers = spark.read.csv(paper_path, header=True)


    # read unpaywall data
    unpaywall = spark.read \
        .parquet("hdfs:///project/core/unpaywall/unpaywall.parquet")\
        .select('doi', 'is_oa', 'oa_status')

    funder_data = spark.read \
        .csv("/project/core/openaire_funders/openaire_funders_clean.csv",
             header=True)

    journal_averages = spark.read \
        .csv("/project/core/bikash_dataset/journal_averages.csv", header=True) \
        .select("journalid", "year", "mean_citations")

    conferences_df = spark \
        .table(db_name + '.conferenceinstances') \
        .select("conferenceinstanceid", "displayname", "papercount",
                "citationcount")

    # average citations per conference instance
    conferences_df = conferences_df \
        .withColum("mean_citations",
                   conferences_df.citationcount / conferences_df.papercount)


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
        "date", "publisher", "journalid", "conferenceinstanceid",
        "referencecount","citationcount", "originalvenue", "familyid"
    )


    # join with unpaywall -------
    logger.info('Joining with unpaywall')
    with_oa = sdg_papers_selected \
        .join(unpaywall, ['doi'], how='left')

    # join with funder data -------------
    sdg_dois = with_oa.select("paperid", "doi")

    # create new col
    # https://stackoverflow.com/a/48984783/3149349
    funder_data = funder_data.withColumn("is_funded", f.lit(True))

    # only keep funded status for now to be included in the papers dataset
    funded_status = sdg_dois.join(funder_data.select("doi", "is_funded"), "doi", how="left")

    # fill sdg articles without funding with "false"
    funded_status = funded_status.fillna({'is_funded': False})

    # join with the oa information
    with_funded_status = with_oa.join(funded_status, ["doi", "paperid"], how="left")

    # also restrict the funder dataset to our papers
    funder_data_in_sdg_set = funder_data.join(sdg_dois, "doi", how="inner")

    # normalise citations (by dividing raw citations by mean citations per
    # journal and year
    norm_citations = with_funded_status \
        .join(journal_averages, ["journalid", "year"], "left") \
        .join(conferences_df, "conferenceinstanceid", "left") \
        .withColumn("citations_norm",
                    f.col("citationcount") / f.col("mean_citations"))

    out_file = path.join(cfg['hdfs']['onmerrit_dir'],
                         "sdg_papers_collated.csv")

    logger.info('Writing paper table to file...')
    norm_citations. \
        drop_duplicates(). \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    out_path = "/project/core/openaire_funders/openaire_funders_injoin_w_sdg.csv"
    logger.info('Writing paper table to file...')
    funder_data_in_sdg_set. \
        write.csv(out_path, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
