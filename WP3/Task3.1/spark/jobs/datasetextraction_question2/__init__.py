'''
Extracts data from MAG to support answer to dataset selection question 2:

"How are papers distributed across scientific disciplines?
 How are the papers from our 3 disciplines (agriculture, climate and health) represented for each of our institutions?
  How many publications and staff we have working in those disciplines?"

This program just extracts relevant data from MAG and stores that as a csv. Another program has to be called on top of
the dataset to get the answer to those questions.

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
    logger.info('Exporting data to support answer to dataset selection query 2: '
                'How are papers distributed across scientific disciplines and how the disciplines from our 3 use cases are represented? How many publications and staff we have working in those disciplines? ' +
                'This program just retrieves the dataset from MAG and stores that as a csv. Another program has to be called on top of that dataset to analyze the data.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # tables that will be used =============================================== #
    aff_df = ss.table(db_name + '.affiliations')  # this will be used to identify institutions
    paper_author_aff_df = ss.table(db_name+'.paperauthoraffiliations')  # this will be used to identify papers coming from an institution
    paper_df = ss.table(db_name+'.papers')  # this will give the details of a paper.

    '''paper_fieldsofStudy = ss.table(db_name+'.paperfieldsofstudy')  # this will give the domain id of the paper -- domain id for agriculture, climate, health or something else
    fieldsofstudy_df = ss.table(db_name+'.fieldsofstudy') # this contains the mapping of domain id to domain name.
    fieldofstudychildren_df = ss.table(db_name + '.fieldofstudychildren')  # this contains the subdomain for given domain.'''


    # udf functions
    # mag_normalisation_institution_names_udf = F.udf(lambda z: mag_normalisation_institution_names(z), StringType())

    total_result = None

    for country, institutions in cfg['data']['institutions_by_country'].items():
        for institution in institutions:
            institution_df = aff_df.filter((aff_df.normalizedname == mag_normalisation_institution_names(institution)) | (aff_df.displayname == institution))
            institution_ids = list(set([int(row.affiliationid) for row in  institution_df.select('affiliationid').collect()]))

            institution_paper_and_author_df = paper_author_aff_df.filter(paper_author_aff_df.affiliationid.isin(institution_ids)).select('paperid', 'authorid').distinct()
            # info of paper with authors and their date of publication for paper published by the institution
            institution_paper_author_dop_df = institution_paper_and_author_df.join(paper_df, ['paperid'], 'inner').select('paperid', 'authorid', 'date')
            logger.info("\n\nExtracted dataset for country : " + country + ", institution : " + institution + ". \n\n")

            # save the data for the current institution
            output_filename = join(cfg['hdfs']['onmerrit_dir'],'ds_q4/' + country + "/" + institution + '_authors.csv')
            institution_paper_author_dop_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
            logger.info("\n\nWrote dataset for country : " + country + ", institution : " + institution + " to file " + output_filename + "\n\n")


            # Update the total_result with info from this loop. Need to add the relevant info.
            institution_paper_author_dop_df = institution_paper_author_dop_df.withColumn('country_name', F.lit(country))
            institution_paper_author_dop_df = institution_paper_author_dop_df.withColumn('institution_name', F.lit(institution))
            if total_result is None:
                total_result = institution_paper_author_dop_df
            else:
                # https://datascience.stackexchange.com/a/27231
                total_result = total_result.union(institution_paper_author_dop_df.select(total_result.columns))



    # Lets write the total result to file
    LogUtils().describe_df(total_result, 'Final dataset')
    all_output_filename = join(cfg['hdfs']['onmerrit_dir'], 'ds_q4/' + 'all_authors.csv')
    total_result.write.csv(all_output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
    logger.info("\n\n\nWrote total results to file "+all_output_filename)
    # done =================================================================== #