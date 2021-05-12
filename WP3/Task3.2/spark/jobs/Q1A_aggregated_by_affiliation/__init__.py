


import sys
import logging
from os.path import join
from typing import List, Any, Iterable, Set
import requests

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import sum,avg,max,min,mean,count,col,first

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


    paperauthoraffiliations = ss.table(db_name+".paperauthoraffiliations").select("paperid", "affiliationid")
    affiliations = ss.table(db_name+".affiliations").select("affiliationid", "latitude", "longitude", "normalizedname", "officialpage", "displayname", "rank")

    authoraffiliations = paperauthoraffiliations.join(affiliations, ["affiliationid"])

    q1a_latlon = authoraffiliations.join(q1a, authoraffiliations.paperid==q1a.source_paperid)



    q1a_latlon_agg = q1a_latlon.distinct()\
        .groupby("affiliationid")\
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("source_paperid"),
             F.mean("ref_oa_score"),
             F.mean("ref_green_score"),
             F.mean("ref_gold_score")
        )


    q1a_latlon_agg.write.csv("hdfs:///project/core/Q1A_ref_oa")


    q1a_latlon_agg = q1a_latlon.filter(F.col("year")<2011).filter(F.col("year")>=2006).distinct()\
        .groupby("affiliationid")\
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("source_paperid"),
             F.mean("ref_oa_score"),
             F.mean("ref_green_score"),
             F.mean("ref_gold_score")
        )

    q1a_latlon_agg.write.csv("hdfs:///project/core/Q1A_ref_oa_2006_2011")

    q1a_latlon_agg = q1a_latlon.filter(F.col("year") <= 2015).filter(F.col("year") >= 2011).distinct() \
        .groupby("affiliationid") \
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("source_paperid"),
             F.mean("ref_oa_score"),
             F.mean("ref_green_score"),
             F.mean("ref_gold_score")
             )

    q1a_latlon_agg.write.csv("hdfs:///project/core/Q1A_ref_oa_2011_2015")

    q1a_latlon_agg = q1a_latlon.filter(F.col("year") > 2015).filter(F.col("year") >= 2020).distinct() \
        .groupby("affiliationid") \
        .agg(F.first("latitude"),
             F.first("longitude"),
             F.first("normalizedname"),
             F.first("rank"),
             F.count("source_paperid"),
             F.mean("ref_oa_score"),
             F.mean("ref_green_score"),
             F.mean("ref_gold_score")
             )

    q1a_latlon_agg.write.csv("hdfs:///project/core/Q1A_ref_oa_2015_2020")


