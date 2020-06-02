'''
Q1: What % of papers coming from a university are OA.

Extracts dataset of OA status of all papers of all the universities listed in THE WUR.

A separate csv will be generated for each country with records of all papers of all THE WUR universities in that country.

Important considerations about this work :
1. The mucc dataset doesn't have distinct entries for the ['paperid','link'] subset; i.e. the same paperid could have different/multiple links or even no link vs some link. I will infer that the paper is OA if there is atleast one link.
2. MAG can generate duplicate entries for papers published by any university because the same paperid could belong to different authors from the same university. This dataset has been prepared to not contain such duplicate paperids(authorship) from same university.  Papers with authorship from multiple universities are counted once towards each of the university concerned.
3. The mucc dataset (for some reasons) doesn't contain entry for all paperids of MAG. This implies that some of the records in the dataset extracted here will not have neither true nor false value for the is_OA field.
4. Only those records marked as true for is_OA field can be said to be OA. Others with false or no value for is_OA field are unknown status (i.e. not necessarily closed access).
'''


import sys
import logging
from os.path import join

from pyspark.sql import functions as F


from shared.log_utils import LogUtils
from shared.MAG_utils import mag_normalisation_institution_names, get_mag_field_ids, mag_normalisation_wiki_link_udf
from shared.udf_utils import array_to_string_udf


__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'







# ============================================================================ #
# MAIN FUNCTION -- name of the task is datasetextraction_question1  #
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
    logger.info('Extracting dataset of OA status for all papers published by all THE WUR universities in the countries of our choice.')

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
    oa_flag = F.when(F.size("list_links")>0, True).otherwise(False)  # considered to be OA is any one link is found. F.size gives the length of the list.
    mucc_df = mucc_df.withColumn("is_OA", oa_flag)


    # tables that will be used
    papers_df = ss.table(db_name + '.papers').select(['paperid', 'year']).drop_duplicates()  # this will be used to extract publication year for papers.

    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_df = country_papers_df.withColumn('normalizedwikiname', mag_normalisation_wiki_link_udf(country_papers_df["wikipage"]))

        country_papers_oa_citations_df = None

        for univ_name in univ_names:
            univ_name_normalised = mag_normalisation_institution_names(univ_name)

            '''
            The dataframe that will be selected for the current univ is either :
            1. When the MAG normalizedname column matches to THE_univ_name_normalised
            or
            2. When the MAG normalised(wikiname) matches to THE_univ_name_normalised -- this matches English names (in MAG wiki links as well as THE) of non English name (in MAG normalisedname or displayname) universities.
            '''
            univ_papers_df_set1 = country_papers_df[country_papers_df['normalizedname'] == univ_name_normalised]

            univ_papers_df_set2 = country_papers_df[country_papers_df['normalizedwikiname'] == univ_name_normalised]

            # The records in two sets can be exactly the same
            # Concat and remove exact duplicates
            univ_papers_df = univ_papers_df_set1.union(univ_papers_df_set2)
            univ_papers_df = univ_papers_df.dropDuplicates()

            # Get OA status
            univ_papers_oa_df = univ_papers_df.join(mucc_df, ['paperid'], how='left_outer')  # left outer join so as to preserve all and only papers of the univ in the result. inner join won't be good because (currently) mucc does not have entries for some paperids

            # Get publication year for each of the papers in the dataset.
            univ_papers_oa_df = univ_papers_oa_df.join(papers_df, ['paperid'], how='inner')

            # Update the total results for the country
            if country_papers_oa_citations_df is None:
                country_papers_oa_citations_df = univ_papers_oa_df
            else:
                # https://datascience.stackexchange.com/a/27231
                country_papers_oa_citations_df = country_papers_oa_citations_df.union(univ_papers_oa_df.select(country_papers_oa_citations_df.columns))

            logger.info("\nExtracted dataset for the university: " + univ_name)

        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "oa_status_" + country_name + "_papers.csv" )
        # csv format doesn't support writing arrays; need to be converted to string representation
        country_papers_oa_citations_df = country_papers_oa_citations_df.withColumn('url_lists_as_string', array_to_string_udf(country_papers_oa_citations_df["list_links"]))
        country_papers_oa_citations_df = country_papers_oa_citations_df.drop("list_links")


        country_papers_oa_citations_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country: " + country_name + " to file " + output_filename + "\n\n")




    logger.info("\n\n\nTask Complete.")
    # done =================================================================== #