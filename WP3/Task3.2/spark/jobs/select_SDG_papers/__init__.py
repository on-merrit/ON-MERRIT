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
    authors = ss \
        .table(db_name + '.authors') \
        .select(['authorid', 'normalizedname', 'displayname',
                 'lastknownaffiliationid', 'papercount'])\
        .withColumnRenamed('displayname', 'author_displayname') \
        .withColumnRenamed('normalizedname', 'author_normalizedname')

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
    sdg_papers = sdg_papers.filter((sdg_papers.year > 2007) & (sdg_papers.year < 2019))

    logger.info("Selected all papers.")

    # logger.info("Printing the number of papers we found:\n")
    # print out what we sampled
    # sdg_papers.groupby(sdg_papers.fieldofstudyid).count().show()

    # write papers to file
    paper_filename = path.join(cfg['hdfs']['onmerrit_dir'], "sdg_papers.parquet")

    # check whether path to output already exists
    # https://stackoverflow.com/a/48708649/3149349
    fs = ss._jvm.org.apache.hadoop.fs.FileSystem.get(ss._jsc.hadoopConfiguration())
    papers_exist = fs.exists(ss._jvm.org.apache.hadoop.fs.Path(paper_filename))

    if not papers_exist:
        logger.info('Writing papers to file...')
        sdg_papers. \
            write.csv(paper_filename, mode="error", header=True,
                      sep=",", quoteAll=True)
    else:
        logger.info('Papers file already exists.')

    # Find all authors of the papers
    refs_to_authors_and_affils = sdg_papers \
        .select(['paperid']) \
        .join(paper_author_affil, ['paperid'], how='left')

    # keep only first and last authors
    # using this stackoverflow as template: https://stackoverflow.com/a/48830780/3149349
    w = Window.partitionBy('paperid')

    first_authors = refs_to_authors_and_affils \
        .withColumn('min_authorsequencenumber',
                    f.min('authorsequencenumber').over(w)) \
        .where(f.col('authorsequencenumber') == f.col('min_authorsequencenumber')) \
        .drop('min_authorsequencenumber')

    last_authors = refs_to_authors_and_affils \
        .withColumn('max_authorsequencenumber',
                    f.max('authorsequencenumber').over(w)) \
        .where(f.col('authorsequencenumber') == f.col('max_authorsequencenumber')) \
        .drop('max_authorsequencenumber')

    first_and_last_authors = first_authors.union(last_authors) \
        .drop_duplicates()

    # Take a paper for validation
    validation_paper_id = 2039052682
    # this paper has these authors:
    # M Slupski ,
    # K Szadujkis-Szadurska ,
    # R Szadujkis-Szadurski ,
    # M Jasinski ,
    # G Grzesk
    # but it should only have the first and the last left

    validation_paper = first_and_last_authors \
        .where(f.col('paperid') == validation_paper_id)

    # check output interactively
    print(validation_paper.show())

    if validation_paper.count() > 2:
        raise AssertionError

    # if all went well, join with full author table and continue
    sdg_authors = first_and_last_authors.join(authors, ['authorid'],
                                              how='left')

    # write authors to file
    author_filename = path.join(cfg['hdfs']['onmerrit_dir'],
                                "sdg_authors.parquet")

    authors_exist = fs.exists(ss._jvm.org.apache.hadoop.fs.Path(author_filename))

    if not authors_exist:
        logger.info('Writing authors to file...')
        sdg_authors. \
            write.csv(author_filename, mode="error", header=True,
                      sep=",", quoteAll=True)
    else:
        logger.info('Authors file already exists.')

    logger.info('Done.')
