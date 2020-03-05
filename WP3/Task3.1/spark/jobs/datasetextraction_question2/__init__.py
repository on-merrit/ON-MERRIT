'''
Extracts dataset of all papers for all the universities listed in THE WUR with the information of fieldofstudy they belong to.

A separate csv will be generated for each country with records of all papers of all THE WUR universities in that country.

Important considerations about this work :
1. MAG can associate a paper to multiple fieldofstudyid. If a paper belongs to more than one of our fieldofstudyid, I create separate records for the paper with each of those fieldofstudyid.
2. MAG can generate duplicate entries for papers published by any university because the same paperid could belong to different authors from the same university. This dataset has been prepared to not contain such duplicate paperids(authorship) from same university. Papers with authorship from multiple universities are counted once towards each of the university concerned.
3. MAG assigns paperfieldofstudyid to every paper but with a 'score' ranging from 0 to 0.999. I will preserve the record whose score is more that 0.5 for any given fieldofstudy it belongs to.
4. The resulting dataset will contain fieldofstudy for the papers. An external program will be used to determine those that belong to the 3 disciplines of our choice.
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
# MAIN FUNCTION -- name of the task is datasetextraction_question2  #
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
    logger.info('Extracting dataset of field of study for all papers published by all THE WUR universities in the countries of our choice.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']


    # tables that will be used
    papers_df = ss.table(db_name + '.papers').select(['paperid', 'year']).drop_duplicates()  # this will be used to identify publication year for papers.
    papersfieldofstudy_df = ss.table(db_name + '.paperfieldsofstudy').drop_duplicates(['paperid', 'fieldofstudyid'])  # this will be used to extract fieldofstudyid for papers.
    papersfieldofstudy_df = papersfieldofstudy_df.where(papersfieldofstudy_df.score>=0.5)  # keep only those records for which the score is more confident


    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_df = country_papers_df.withColumn('normalizedwikiname', mag_normalisation_wiki_link_udf(country_papers_df["wikipage"]))

        country_papers_fsid_df = None

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

            # Get fieldofstudyid for the papers
            univ_papers_fsid_df = univ_papers_df.join(papersfieldofstudy_df, ['paperid'], how='inner')

            # Update the total results for the country
            if country_papers_fsid_df is None and len(univ_papers_fsid_df.head(1)) > 0:
                country_papers_fsid_df = univ_papers_fsid_df
            else:
                # https://datascience.stackexchange.com/a/27231
                if len(univ_papers_fsid_df.head(1)) > 0:
                    country_papers_fsid_df = country_papers_fsid_df.union(univ_papers_fsid_df.select(country_papers_fsid_df.columns))

            logger.info("\nExtracted dataset for the university: " + univ_name)

        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "fsid_" + country_name + "_papers.csv" )


        country_papers_fsid_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country: " + country_name + " to file " + output_filename + "\n\n")




    logger.info("\n\n\nTask Complete.")
    # done =================================================================== #