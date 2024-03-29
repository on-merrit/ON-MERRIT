import sys
import logging
from os.path import join
from typing import List, Any, Iterable, Set
import requests

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext, SQLContext

# Don't worry about this import being red flagged by PyCharm; it seems to be OK in pyspark execution
from shared.log_utils import LogUtils

import unicodedata
import re


# ============================================================================ #
# MAIN FUNCTION                                                                #
# ============================================================================ #
def analyze(ss, cfg):
    """
    Run job 
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """
    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Exporting data to support answer to dataset_selection_query_1 : What % of papers coming from a ' +
                'university are OA. This program just retrieves dois of all papers published by the input university ' +
                'and saves it to a file. Another program has to be called on top of this dataset to answer the question'
                )

    # MAG dataset to use
    db_name = "mag2020"
    sql_sc = SQLContext(ss)
    q1a = sql_sc.read.parquet("hdfs:///project/core/Q1A_aggregated_ref")

    paperauthoraffiliations = ss.table(db_name + ".paperauthoraffiliations").select("paperid", "affiliationid")
    affiliations = ss.table(db_name + ".affiliations").select("affiliationid", "latitude", "longitude",
                                                              "normalizedname", "officialpage", "displayname", "rank")

    authoraffiliations = paperauthoraffiliations.join(affiliations, ["affiliationid"])

    q1a_aff = authoraffiliations.join(q1a, authoraffiliations.paperid == q1a.source_paperid)

    q1a_aff = q1a_aff.distinct() \
        .groupby("year","affiliationid") \
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("source_paperid"),
             F.mean("ref_oa_score"),
             F.mean("ref_green_score"),
             F.mean("ref_gold_score")
             )

    q1a_aff[q1a_aff.year>2000].write.csv("hdfs:///project/core/Q1A_ref_historical")

    q1a_raw = sql_sc.read.parquet("hdfs:///project/core/Q1A_raw")
    unpaywall = sql_sc.read.parquet("hdfs:///project/core/unpaywall/unpaywall.parquet")\
        .withColumnRenamed("is_oa","source_is_oa")\
        .withColumnRenamed("oa_status", "source_oa_status")

    q1a_source_oa = q1a_raw.join(unpaywall, q1a_raw.source_doi == unpaywall.doi, "left").select("source_paperid", "source_doi",
                                                                                        "source_year", "source_is_oa",
                                                                                        "source_oa_status").distinct()



    q1a_source_oa_latlon = authoraffiliations.join(q1a_source_oa, authoraffiliations.paperid == q1a_source_oa.source_paperid)

    q1a_source_oa_agg = q1a_source_oa_latlon.distinct() \
        .groupby("source_year","affiliationid") \
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("paperid").alias("count_paper"),
             F.sum(F.when(F.col("source_is_oa") == True, 1).otherwise(0)).alias("count_oa"),
             F.sum(F.when(F.col("source_oa_status") == "green", 1).otherwise(0)).alias("count_green"),
             F.sum(F.when(F.col("source_oa_status") == "gold", 1).otherwise(0)).alias("count_gold"))
    q1a_source_oa_agg = q1a_source_oa_agg.withColumn("source_oa_score", F.col("count_oa") / F.col("count_paper"))
    q1a_source_oa_agg = q1a_source_oa_agg.withColumn("source_oa_gold", F.col("count_gold") / F.col("count_oa"))
    q1a_source_oa_agg = q1a_source_oa_agg.withColumn("source_oa_green", F.col("count_green") / F.col("count_oa"))

    q1a_source_oa_agg[q1a_source_oa_agg.source_year>2000].write.csv("hdfs:///project/core/Q1A_src_historical")