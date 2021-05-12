"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
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
        .table(db_name + '.papers')
    journals_df = spark \
        .table(db_name + '.journals') \
        .select("journalid", "normalizedname", "displayname", "publisher")
    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    # the maximum of the authorsequencenumber should be the number of authors a
    # given paper has
    # how to do the aggregation: https://stackoverflow.com/a/36251274/3149349
    coauthors_per_paper = paper_author_affil \
        .groupBy("paperid") \
        .agg(f.max(f.col("authorsequencenumber")).alias("n_authors"))

    papers_with_coauthors = papers_df.join(coauthors_per_paper, "paperid", "left")

    # compute average citations across journals and years
    aggregated_citations = papers_with_coauthors \
        .groupBy("journalid", "year") \
        .agg(f.avg(f.col("citationcount")).alias("mean_citations"),
             f.avg(f.col("n_authors")).alias("mean_authors"))

    journal_table = journals_df.join(aggregated_citations, "journalid", "left")

    logger.info('Writing averaged coauthors and citations to file...')
    out_file = "/project/core/bikash_dataset/journal_averages.csv"
    journal_table. \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
