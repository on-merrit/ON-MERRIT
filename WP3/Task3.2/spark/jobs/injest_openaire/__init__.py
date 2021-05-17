"""
This script takes a sample of papers from climate, health and agriculture for further inspection.
"""


import sys
import logging
from pyspark.sql.functions import col, explode


def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Injesting openaire funder data dump')

    # avoid nazis
    spark = ss

    logger.info('Reading the bunch')

    # https://stackoverflow.com/a/32233865/3149349
    path = "/project/core/openaire_funders/*/*.json.gz"
    funder_data = spark.read.json(path)

    logger.info('Print the schema')
    funder_data.printSchema()

    # explode the doi column
    logger.info('Get the DOIs')
    explodeDF = funder_data.select(explode("pid").alias("pid"), "projects")
    flattenDF = explodeDF.selectExpr("pid.scheme", "pid.value", "projects")
    only_dois = flattenDF.filter(flattenDF.scheme == "doi")
    only_dois.printSchema()

    # get the projects per doi together
    logger.info('Get the funding info per DOI')
    projects = only_dois.select(col("value").alias("doi"),
                                explode("projects").alias("p"))
    projects_flat = projects.selectExpr("doi", "p.funder", "p.title")

    funders_exploded = projects_flat.selectExpr(
        "doi", "title", "funder.fundingStream", "funder.jurisdiction",
        "funder.name", "funder.shortName")

    funders_renamed = funders_exploded \
        .withColumnRenamed("title", "funded_project_title") \
        .withColumnRenamed("jurisdiction", "funding_jurisdiction") \
        .withColumnRenamed("name", "funder_name") \
        .withColumnRenamed("shortName", "funder_shortname")
    funders_renamed.printSchema()

    logger.info('Writing cleaned funding data to file...')
    out_file = "/project/core/openaire_funders/openaire_funders_clean.parquet"
    funders_renamed. \
        write.csv(out_file, mode="overwrite", header=True, sep=",",
                  quoteAll=True)

    logger.info('Done.')
