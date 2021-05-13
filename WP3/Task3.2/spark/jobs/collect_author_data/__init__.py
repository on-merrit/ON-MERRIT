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
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_first_last_authors.csv")
    sdg_authors = spark.read.csv(author_filename, header=True)

    # also get the table of links between authors and papers
    paper_author_affil = spark \
        .table(db_name + '.paperauthoraffiliations')

    # also get all papers
    papers_df = spark \
        .table(db_name + '.papers') \
        .select(['paperid', 'year', 'citationcount', 'journalid']) \
        .drop_duplicates()

    # and get the journal averages
    journal_averages = spark.read \
        .csv("/project/core/bikash_dataset/journal_averages.csv", header=True) \
        .select("journalid", "year", "mean_citations", "mean_authors")

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
        .select(['authorid', 'paperid', 'year', 'citationcount', 'journalid'])

    # we want to find the first year (i.e. paper) per author
    # get min year per author
    first_papers = all_papers \
        .groupby('authorid') \
        .min('year') \
        .select('authorid', f.col('min(year)').alias('year_first_paper'))

    # merge the newly generated information to our table
    full_author_table = sdg_authors \
        .join(first_papers, ['authorid'], how='left')

    # get total citations per author and normalise
    citations = all_papers \
        .join(journal_averages, ["journalid", "year"], "left") \
        .withColumn("citations_norm",         # normalise citations on paper level
                    f.col("citationcount") / f.col("mean_citations")) \
        .groupBy("authorid") \
        .agg(f.sum(f.col("citationcount")).alias("n_citations"),
             f.sum(f.col("citations_norm")).alias("n_citations_norm"))

    full_author_table = full_author_table \
        .join(citations, "authorid", how="left")

    # split our table in two. currently we have a table with one row per
    # publication of our author. this is a lot of duplication
    # new tables: table a with affiliation data per paper
    # table b with author level information
    sdg_paper_author_affiliations = full_author_table \
        .select("authorid", "paperid", "affiliationid", "authorsequencenumber",
                "originalauthor", "originalaffiliation",
                "author_normalizedname")

    sdg_author_table = full_author_table \
        .select("authorid", "author_normalizedname", "author_displayname",
                "lastknownaffiliationid", "papercount", "year_first_paper",
                "n_citations", "n_citations_norm") \
        .drop_duplicates()

    logger.info('Writing author table to file...')
    out_file = path.join(cfg['hdfs']['onmerrit_dir'],
                         "sdg_author_data.csv")
    sdg_author_table. \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    sdg_paper_author_affiliations \
        .write.csv("/project/core/bikash_dataset/sdg_author_paper_affil.csv",
                   mode="overwrite", header=True, sep=",", quoteAll=True)

    logger.info('Done.')
