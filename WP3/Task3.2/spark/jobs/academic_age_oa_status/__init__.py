"""
This script merges the datasets on author ids and OA status of papers.
Todo: adapt script further so it merges the two csv files per country
"""


import sys
import logging
from typing import List
from os.path import join

from pyspark import SparkContext, SQLContext

from shared.log_utils import LogUtils

def analyze(ss, cfg):
    """
    Run job
    :param ss: SparkSession
    :param cfg: app configuration
    :return: None
    """

    logger = logging.getLogger(__name__)
    logger.info('Python version: {}'.format(sys.version))
    logger.info('Starting to merge files ...')


    total_result_df = None

    for country_name, univ_names in cfg['data']['all_THE_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_oa_df = country_papers_df.join(mucc_df, ['paperid'], how='left_outer')  # left outer join so as to preserve all and only papers of the country in the result. inner join won't be good because (currently) mucc does not have entries for some paperids

        country_papers_oa_df = country_papers_oa_df.join(papers_df, ['paperid'], how='inner')  # add in the information about publication year of each papers.



        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "OA_status_"+country_name + "_papers.csv" )
        # csv format doesn't support writing arrays; need to be converted to string representation
        country_papers_oa_df = country_papers_oa_df.withColumn('url_lists_as_string', array_to_string_udf(country_papers_oa_df["list_links"]))
        country_papers_oa_df = country_papers_oa_df.drop("list_links")


        country_papers_oa_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country : " + country_name + " to file " + output_filename + "\n\n")

        # Update the total_result with info from this loop. Need to add the relevant country name.
        country_papers_oa_df = country_papers_oa_df.withColumn('country_name', F.lit(country_name))

        if total_result_df is None:
            total_result_df = country_papers_oa_df
        else:
            # https://datascience.stackexchange.com/a/27231
            total_result_df = total_result_df.union(country_papers_oa_df.select(total_result_df.columns))





    # Lets write the total dataset to file
    LogUtils().describe_df(total_result_df, 'Countries All Papers with OA status info dataset ')
    total_output_filename = join(cfg['hdfs']['onmerrit_dir'], 'OA_status_all_countries_all_papers.csv')
    total_result_df.write.csv(total_output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
    logger.info("\n\n\nWrote the affiliation_id_lat_long dataset to file "+total_output_filename)
    # done =================================================================== #