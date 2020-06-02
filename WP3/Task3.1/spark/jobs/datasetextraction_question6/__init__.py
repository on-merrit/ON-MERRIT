'''
Q6: Distribution of references (outgoing) for open access articles vs subscription based articles?

Extracts dataset of count of all OA and unknown papers referenced by all papers published by all the universities listed in THE WUR.
# (University A publishes paper X which references m papers; how many of those m papers are OA/non-OA)
The references are whatever is identified by MAG.

A separate csv will be generated for each country with records of all papers of all THE WUR universities in that country.

Important considerations about this work :
1. The mucc dataset doesn't have distinct entries for the ['paperid','link'] subset; i.e. the same paperid could have different/multiple links or even no link vs some link. I will infer that the paper is OA if there is atleast one link.
2. MAG can generate duplicate entries for papers published by any university because the same paperid could belong to different authors from the same university. This dataset has been prepared to not contain such duplicate paperids(authorship) from same university. Papers with authorship from multiple universities are counted once towards each of the universities concerned.
3. The mucc dataset (for some reasons) doesn't contain entry for all paperids of MAG. This implies that some of the records in the dataset extracted here will not have neither true nor false value for the is_OA field.
4. Only those records marked as true for is_OA field can be said to be OA. Others with false or no value for is_OA field are unknown status (i.e. not necessarily closed access).
5. The count of papers (paperid) extracted here for a university can be less than in other datasets because here only those paperids for which some references were available in the MAG are retained. For eg: paperid 1000193646 has no references in the MAG paperreferences table and therefore is not included in this dataset.
'''


import sys
import logging
from os.path import join

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


from shared.log_utils import LogUtils
from shared.MAG_utils import mag_normalisation_institution_names, get_mag_field_ids, mag_normalisation_wiki_link_udf
from shared.udf_utils import array_to_string_udf


__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'



def has_column(df, col):
    '''
    https://stackoverflow.com/a/36332079/530399  -- returns true if the dataframe has the input columnname as its field.
    I am using this to check before calling the fillna() which throws exception if the column doesn't exist
    :param df:
    :param col:
    :return:
    '''
    try:
        df[col]
        return True
    except AnalysisException:
        return False


# ============================================================================ #
# MAIN FUNCTION -- name of the task is datasetextraction_question6  #
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
    logger.info('Extracting dataset for count of references (outgoing) for open access articles vs subscription based articles.')

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
    papers_df = ss.table(db_name + '.papers').select(['paperid', 'year']).drop_duplicates()  # this will be used to identify publication year for papers.
    paperreferences_df = ss.table(db_name + '.paperreferences').select(['paperid', 'paperreferenceid']).drop_duplicates()  # this will be used to identify publication year for papers.


    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_df = country_papers_df.withColumn('normalizedwikiname', mag_normalisation_wiki_link_udf(country_papers_df["wikipage"]))

        country_papers_oa_references_df = None

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

            # Get year of publication for each of the papers in the dataset.
            univ_papers_df = univ_papers_df.join(papers_df, ['paperid'], how='inner')

            # Get the references for each paper in the dataset
            univ_papers_references_df = univ_papers_df.join(paperreferences_df, ['paperid'], how='inner')
            univ_papers_references_df = univ_papers_references_df.dropDuplicates()


            # Get OA status for each of the referenced paper
            univ_papers_references_oastatus_df = univ_papers_references_df.join(mucc_df, univ_papers_references_df.paperreferenceid == mucc_df.paperid, how='left_outer')  # left outer join so as to preserve all and only papers of the univ in the result. inner join won't be good because (currently) mucc does not have entries for some paperids
            # So as not to confuse the paperid of the original paper with the paperid of the referenced paper (which has already been captured in the paperreferenceid field)
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.drop(mucc_df.paperid)
            # Also replace empty values for is_OA filed of MUCC with false -- null value is not allowed for pivoting that needs to be done later
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.fillna({'is_OA': False})


            # Rename appropriately the fields pertaining to referenced papers and remove unnecessary ones.
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.withColumnRenamed("is_OA", "is_referencedpaper_OA")
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.drop("list_links")


            # https: // sparkbyexamples.com / spark / how - to - pivot - table - and -unpivot - a - spark - dataframe /
            # The second (optional) argument to pivot is the list of distinct values possible in the pivoting column -- in this case, its the OA status flags True or False. It helps to speed up.
            univ_papers_references_oacount_df = univ_papers_references_oastatus_df.select(['paperid', 'is_referencedpaper_OA']).groupBy('paperid').pivot("is_referencedpaper_OA", [True,False]).count()
            # LogUtils().describe_df(univ_papers_references_oacount_df, 'Pivot dataset ')
            # Also replace null values for any pivoted column with 0. e.g. the true column can be null for a paper if all its references are unknown access.
            # fillna will create problems when dataframe don't have the specified columns -- eg: when no paper was found for the university
            if has_column(univ_papers_references_oacount_df, "true"):
                univ_papers_references_oacount_df = univ_papers_references_oacount_df.fillna({'true': 0})
            if has_column(univ_papers_references_oacount_df, "false"):
                univ_papers_references_oacount_df = univ_papers_references_oacount_df.fillna({'false': 0})


            # Rename appropriately
            univ_papers_references_oacount_df = univ_papers_references_oacount_df.withColumnRenamed("true", "count_OA_references")
            univ_papers_references_oacount_df = univ_papers_references_oacount_df.withColumnRenamed("false", "count_unknown_references")





            # The paperreferenceid and the is_referencedpaper_OA fileds should be removed now and the univ_papers_references_oastatus_df should be deduplicated.
            univ_papers_references_oastatus_df  = univ_papers_references_oastatus_df.drop("paperreferenceid")
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.drop("is_referencedpaper_OA")
            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.dropDuplicates()

            univ_papers_references_oastatus_df = univ_papers_references_oastatus_df.join(univ_papers_references_oacount_df, ['paperid'], how='inner')
            # LogUtils().describe_df(univ_papers_references_oastatus_df, 'University dataset')


            # Update the total results for the country
            if country_papers_oa_references_df is None and len(univ_papers_references_oastatus_df.head(1)) > 0:
                country_papers_oa_references_df = univ_papers_references_oastatus_df
            else:
                # https://datascience.stackexchange.com/a/27231
                if len(univ_papers_references_oastatus_df.head(1)) > 0:
                    country_papers_oa_references_df = country_papers_oa_references_df.union(univ_papers_references_oastatus_df.select(country_papers_oa_references_df.columns))

            logger.info("\nExtracted dataset for the university: " + univ_name)

        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "rc_oa_" + country_name + "_papers.csv")
        country_papers_oa_references_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country: " + country_name + " to file " + output_filename + "\n\n")




    logger.info("\n\n\nTask Complete.")
    # done =================================================================== #