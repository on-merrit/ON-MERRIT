'''
Extracts dataset of OA status of all papers of all the institutions (i.e. all universities and all other institutions in MAG and not just the universities listed in THE WUR)
in the countries of our choice. Further processing will be needed to make analysis per THE WUR univ names.

A separate csv will be generated for each country. A total dataset of all records will also be created.

Future Todo :
Breakup the OA proportion by papers per discipline?
Classify the OA status into green, gold and open to read, open licence etc.

Important considerations about this work :
1. The mucc dataset doesn't have distinct entries for the ['paperid','link'] subset; i.e. the same paperid could have different/multiple links or even no link vs some link. I will infer that the paper is OA if there is atleast one link.
2. The country papers can also have duplicates (duplicate paperid) because the same paperid could belong to different authors from multiple university within the same country. It has been prepared to not contain duplicate paperids(authorship) from same university, however. So, at the university level, paperids are non-dup but at the country level, the paperid field can be duplicated.
3. The mucc dataset (for some reasons) doesn't contain entry for all paperids of MAG. This implies that some of the records in the dataset extracted here will not have neither true nor false value for the is_OA field.
4. Only those records marked as true for is_OA field can be said to be OA. Others with false or no value for is_OA field are unknown status (i.e. not necessarily closed access).
'''


import sys
import logging
from os.path import join

from pyspark.sql import functions as F


from shared.log_utils import LogUtils
from shared.MAG_utils import mag_normalisation_institution_names, get_mag_field_ids
from shared.udf_utils import array_to_string_udf


__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'







# ============================================================================ #
# MAIN FUNCTION -- name of the task is datasetextraction_jcdl_OA  #
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
    logger.info('Extracting dataset of OA status of all papers published by all THE universities in the countries of our choice.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # datasets that will be used =============================================== #
    mucc_df = ss.read.parquet(cfg['hdfs']['mucc_dir']).select(['paperid','link'])  # this will be used to determine OA status.
    # The mucc dataset doesn't have distinct entries for the ['paperid','link'] subset; i.e. the same paperid could have different/multiple links or even no link vs some link. I will infer that the paper is OA is there is atleast one link.
    # just retain what are the distinct combinations of subset ['paperid','link']
    mucc_df = mucc_df.distinct()
    # Since the same paper id may have multiple OA links (or marked with null value), lets create a single entry with list of all such links in one row.
    mucc_df = mucc_df.groupby("paperid").agg(F.collect_list("link").alias("list_links"))
    # Add a column for OA flag
    oa_flag = F.when(F.size("list_links")>0, True).otherwise(False)  # considered to be OA is any one link is found. F.size gives the length od the list.
    mucc_df = mucc_df.withColumn("is_OA", oa_flag)


    # tables that will be used
    papers_df = ss.table(db_name + '.papers').select(['paperid', 'year']).drop_duplicates()  # this will be used to extract publication year of the papers.

    total_result_df = None

    for country_name, univ_names in cfg['data']['all_THE_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_oa_df = country_papers_df.join(mucc_df, ['paperid'], how='left_outer')  # left outer join so as to preserve all and only papers of the country in the result. inner join won't be good because mucc may not have an entry for some paperids

        country_papers_oa_df = country_papers_oa_df.join(papers_df, ['paperid'], how='inner')



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