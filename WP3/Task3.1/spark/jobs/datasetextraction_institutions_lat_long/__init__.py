'''
Extracts dataset of intituions with their latitude and longitude from MAG.

The aim is to process this data externally (outside cluster environment) to identify the countries that institutions belong to. MAG only gives lat,long for institutions and not the countries they belong to. We have to use external python libraries to
determine that and it is easy to use such libraries outside the cluster environment.

This program only extracts the dataset of institution_ids of MAGs mapped to their lat,long. Another program has to be used to determine country name based on the lat,long value.

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
# MAIN FUNCTION -- name of the task is datasetextraction_institutions_lat_long  #
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
    logger.info('Extracting dataset of MAG institution ids mapped to their latitude and longitude values.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # tables that will be used =============================================== #
    aff_df = ss.table(db_name + '.affiliations')  # this will be used to identify institutions
    aff_lat_long_df = aff_df.select('affiliationid', 'latitude', 'longitude')



    # Lets write the dataset to file
    LogUtils().describe_df(aff_lat_long_df, 'Affiliation lat long dataset')
    output_filename = join(cfg['hdfs']['onmerrit_dir'], 'affiliation_id_lat_long.csv')
    aff_lat_long_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
    logger.info("\n\n\nWrote the affiliation_id_lat_long dataset to file "+output_filename)
    # done =================================================================== #