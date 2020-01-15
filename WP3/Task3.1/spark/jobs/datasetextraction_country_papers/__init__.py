'''
Extracts dataset of all papers of ALL institutions in the countries of our choice. This includes all the institutions found in MAG for that country.

A separate csv will be generated for each country. A total dataset of all records will also be created.

The aim is to make these datasets (per country) readily available for the dataset selection questions.

Important considerations about this work :

'''


import sys
import logging
from os.path import join

from pyspark.sql import functions as F


from shared.log_utils import LogUtils
from shared.MAG_utils import mag_normalisation_institution_names, get_mag_field_ids


__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'







# ============================================================================ #
# MAIN FUNCTION -- name of the task is datasetextraction_country_papers  #
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
    logger.info('Extracting dataset of all papers published by all institution in the countries of our choice.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # tables that will be used =============================================== #
    aff_df = ss.table(db_name + '.affiliations')  # this will be used to identify institutions
    paper_author_aff_df = ss.table(db_name + '.paperauthoraffiliations')  # this will be used to identify papers coming from an institution


    '''paper_df = ss.table(db_name + '.papers')  # this will give the details of a paper.
    paper_fieldsofStudy = ss.table(db_name+'.paperfieldsofstudy')  # this will give the domain id of the paper -- domain id for agriculture, climate, health or something else
    fieldsofstudy_df = ss.table(db_name+'.fieldsofstudy') # this contains the mapping of domain id to domain name.
    fieldofstudychildren_df = ss.table(db_name + '.fieldofstudychildren')  # this contains the subdomain for given domain.'''


    affiliation_countryname_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], 'affiliations_country.csv'), header=True, mode="DROPMALFORMED")
    LogUtils().describe_df(affiliation_countryname_df, 'Affiliations country name dataset')


    total_result_df = None

    for country_name, _ in cfg['data']['institutions_by_country'].items():
        cnames = cfg['data']['country_names_variations'][country_name]  # possible variations of country names
        cnames.append(country_name)
        logger.info("\n\n\nPossible name variations for " + country_name +" = "+ str(cnames))
        all_institutions_ids_df = affiliation_countryname_df.filter(F.col("country").isin(cnames)).select("affiliationid").distinct()  # all institutions within the country

        country_papers_df = all_institutions_ids_df.join(paper_author_aff_df, all_institutions_ids_df.affiliationid == paper_author_aff_df.affiliationid) # inner join
        country_papers_df = country_papers_df.select('paperid', paper_author_aff_df.affiliationid)  # keep only the necessary fields
        # same paper could have multiple authors within the same institution and therefore have multiple entries in paper_author_aff_df. Need to get rid of such duplicate entries.
        # This will however preserve records with same paperid but different affilaitionid (when the same paper is written by authors from different univs), which is desired.
        country_papers_df = country_papers_df.dropDuplicates()

        # To get back the name of the institutions
        country_papers_institution_df = country_papers_df.join(aff_df, country_papers_df.affiliationid == aff_df.affiliationid) # inner join
        # keep only the necessary fields.
        '''I keep wikipage because they might contain english names for non-english universities. Ex: 
        Universidade Federal de Ciências da Saúde de Porto Alegre has wikipage http://en.wikipedia.org/wiki/Federal_University_of_Health_Sciences_of_Porto_Alegre
        '''
        country_papers_institution_df = country_papers_institution_df.select('paperid', country_papers_df.affiliationid, 'normalizedname', 'displayname', 'wikipage')


        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], country_name + "_papers.csv" )
        country_papers_institution_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country : " + country_name + " to file " + output_filename + "\n\n")

        # Update the total_result with info from this loop. Need to add the relevant info.
        country_papers_institution_df = country_papers_institution_df.withColumn('country_name', F.lit(country_name))

        if total_result_df is None:
            total_result_df = country_papers_institution_df
        else:
            # https://datascience.stackexchange.com/a/27231
            total_result_df = total_result_df.union(country_papers_institution_df.select(total_result_df.columns))





    # Lets write the total dataset to file
    LogUtils().describe_df(total_result_df, 'Countries All Papers with affiliation info dataset ')
    total_output_filename = join(cfg['hdfs']['onmerrit_dir'], 'all_countries_all_papers.csv')
    total_result_df.write.csv(total_output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
    logger.info("\n\n\nWrote the affiliation_id_lat_long dataset to file "+total_output_filename)
    # done =================================================================== #