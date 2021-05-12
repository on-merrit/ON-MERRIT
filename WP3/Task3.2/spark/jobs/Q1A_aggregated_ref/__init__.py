


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
    q1a = sql_sc.read.parquet("hdfs:///project/core/Q1A_raw")

    # "source_paperid", "source_doi", "source_year", "ref_paperid", "ref_doi", "is_oa", "oa_status"
    #q1a_aggregate = q1a.groupby("source_paperid").agg(count("ref_paperid").alias("count_ref"), count("ref_doi").alias("count_doi"),count("is_oa").alias("oa_count"))
    q1a_aggregate_ref = q1a.groupby("source_paperid").agg(
        count("ref_paperid").alias("count_ref"),
        first("source_year").alias("year"),
        F.sum(F.when(F.col("is_oa") == True, 1).otherwise(0)).alias("count_oa_ref"),
        F.sum(F.when(F.col("oa_status") == "green", 1).otherwise(0)).alias("count_green_ref"),
        F.sum(F.when(F.col("oa_status") == "gold", 1).otherwise(0)).alias("count_gold_ref"))

    q1a_aggregate_ref=q1a_aggregate_ref.withColumn("ref_oa_score", col("count_oa_ref")/col("count_ref"))
    q1a_aggregate_ref=q1a_aggregate_ref.withColumn("ref_green_score", col("count_green_ref")/col("count_oa_ref"))
    q1a_aggregate_ref=q1a_aggregate_ref.withColumn("ref_gold_score", col("count_gold_ref")/col("count_oa_ref"))

    q1a_aggregate_ref.write.save("hdfs:///project/core/Q1A_aggregated_ref")