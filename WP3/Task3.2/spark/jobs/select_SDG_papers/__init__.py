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
        .select(['fieldofstudyid', 'displayname', 'normalizedname']) \
        .withColumnRenamed('displayname', 'fos_displayname') \
        .withColumnRenamed('normalizedname', 'fos_normalizedname')
    # ref for renaming cols: https://stackoverflow.com/a/36302241/3149349

    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    osdg_ontology = spark.read. \
        option("sep", ";"). \
        csv('/project/core/OSDG/OSDG-Ontology.csv', header=True)

    # select the SDGs 2, 3 and 13 (hunger, health and climate)
    osdg_ontology = osdg_ontology.select("SDG_label", "fos_id")

    # adapted from:
    # https://stackoverflow.com/questions/35870760/filtering-a-pyspark-dataframe-with-sql-like-in-clause
    sdg_fields = osdg_ontology.where(f.col("SDG_label").isin(["SDG_2", "SDG_3", "SDG_13"]))

    field_of_study_selection = sdg_fields \
        .join(field_of_study,
              sdg_fields.fos_id == field_of_study.fieldofstudyid,
              "left")

    sdg_paper_ids = field_of_study_selection \
        .join(paper_field_of_study, ['fieldofstudyid'], how='left')

    sdg_papers = sdg_paper_ids.join(papers_df, ['paperid'], how='left')

    # only keep papers in our window (2006-2020)
    sdg_papers = sdg_papers \
        .filter((sdg_papers.year > 2005) & (sdg_papers.year <= 2020))

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
