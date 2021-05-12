


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

    # tables that will be used =============================================== #
    paper_df = ss.table(db_name+".papers").select("paperid","doi", "year")
    source_paper_df = paper_df.withColumnRenamed("paperid", "source_paperid").withColumnRenamed("doi", "source_doi").withColumnRenamed("year", "source_year")
    references_df = ss.table(db_name+".paperreferences")
    papers_with_references = source_paper_df.join(references_df, source_paper_df.source_paperid==references_df.paperid, 'left').select("source_paperid", "source_doi", "source_year", "paperreferenceid")
    papers_with_references_and_doi = papers_with_references\
        .join(paper_df, papers_with_references.paperreferenceid==paper_df.paperid, 'left')\
        .withColumnRenamed("doi", 'ref_doi')\
        .withColumnRenamed("paperid", "ref_paperid")\
        .select("source_paperid", "source_doi", "source_year", "ref_paperid", "ref_doi")

    unpaywall = sql_sc.read.parquet("hdfs:///project/core/unpaywall/unpaywall.parquet")

    q1a = papers_with_references_and_doi.join(unpaywall, papers_with_references_and_doi.ref_doi==unpaywall.doi, "left").select("source_paperid", "source_doi", "source_year", "ref_paperid", "ref_doi", "is_oa", "oa_status")

    q1a.write.save("hdfs:///project/core/Q1A_raw")

