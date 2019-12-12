'''
Extracts dataset of all papers of all institutions in the countries of our choice; divided by disciplines of our choice.

A separate csv will be generated for each country for each discipline.

The aim is to make this datasets (papers per country classified into disciplines) readily available for the dataset selection question 2.

Important considerations about this work :
1. A paper may belong to several fields of study -- e.g. agriculture and climatology. This dataset will allow for all such possibilities.
2. If there are multiple occurrence of the same paper within a discipline; only a single copy is kept. For eg: the same paper under "medicine" dataset has 2 entries - one for "cancer" and the other for "anatomy".
3. There is a confidence score assigned to every field_of_study a paper is assigned to. For our selected disciplines, we will only keep those records with score >=0.3
4. We will include papers belonging to all the sub fields of our discipline -- will use the fos_hierarchy.csv file to get subfields of a field. For eg: the medicine dataset will contain papers belonging to fields "cancer", "anatomy" etc.
'''


import sys
import logging
from os.path import join
from itertools import chain

from pyspark.sql import functions as F


from shared.log_utils import LogUtils
from shared.MAG_utils import mag_normalisation_institution_names, get_mag_field_ids


__author__ = 'bikash gyawali'
__email__ = 'bikash.gyawali@open.ac.uk'







# ============================================================================ #
# MAIN FUNCTION -- name of the task is datasetextraction_country_papers_disciplines  #
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
    logger.info('Extracting dataset of all papers published by all institution in the countries of our choice and divided into disciplines.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    # tables that will be used =============================================== #
    paper_fieldsofStudy = ss.table(db_name + '.paperfieldsofstudy')  # this will give the domain id of the paper -- domain id for agriculture, climate, health or something else


    '''aff_df = ss.table(db_name + '.affiliations')  # this will be used to identify institutions
    paper_author_aff_df = ss.table(db_name + '.paperauthoraffiliations')  # this will be used to identify papers coming from an institution
    paper_df = ss.table(db_name + '.papers')  # this will give the details of a paper.
    fieldsofstudy_df = ss.table(db_name+'.fieldsofstudy') # this contains the mapping of domain id to domain name.
    fieldofstudychildren_df = ss.table(db_name + '.fieldofstudychildren')  # this contains the subdomain for given domain.'''


    fos_hierarchy_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'],  'fos_hierarchy.csv'), header=True,  mode="DROPMALFORMED")
    disciplines_ids = {}
    for discipline in cfg['data']['fields_of_study']:
        discipline_df = fos_hierarchy_df.filter((fos_hierarchy_df.normalizedname == discipline)).select('fieldofstudyid', 'child_ids')

        child_fields_ids = [row.child_ids.split(",") for row in discipline_df.select('child_ids').collect()]  # This gives list of list of strings
        child_fields_ids = list(chain.from_iterable(child_fields_ids))  # This flattens the 2d list into 1D. https://stackoverflow.com/a/29244327/530399
        child_fields_ids = [int(x) for x in child_fields_ids]

        field_ids = [row.fieldofstudyid.split(",") for row in discipline_df.select('fieldofstudyid').collect()]  # This gives list of list of strings
        field_ids = list(chain.from_iterable(field_ids))  # This flattens the 2d list into 1D. https://stackoverflow.com/a/29244327/530399
        field_ids = [int(x) for x in field_ids]

        discipline_field_ids = child_fields_ids + field_ids
        disciplines_ids[discipline] = discipline_field_ids



    for country_name, _ in cfg['data']['institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")

        country_papers_fos_df = country_papers_df.join(paper_fieldsofStudy, country_papers_df.paperid == paper_fieldsofStudy.paperid) # inner join
        country_papers_fos_df = country_papers_fos_df.drop(paper_fieldsofStudy.paperid)  # keep only the necessary fields
        LogUtils().describe_df(country_papers_df, country_name+' Papers df')

        # Lets find the subset of papers that belong to each of our disciplines
        for discipline in cfg['data']['fields_of_study']:
            discipline_ids = disciplines_ids[discipline]

            country_papers_discipline_df = country_papers_fos_df.filter(F.col("fieldofstudyid").isin(discipline_ids))
            x_len = country_papers_discipline_df.count()

            # Only keep those papers that are identified to belong to the discipline with a score greater than 0.3 -- others are probably false positives
            country_papers_discipline_df = country_papers_discipline_df.filter(F.col("score")>0.3)
            y_len = country_papers_discipline_df.count()
            logger.info("Removed "+str(x_len-y_len)+" entries for "+country_name+" papers in discipline "+discipline+" with score of less than 0.3")

            # Remove duplicates within this discipline
            country_papers_discipline_df = country_papers_discipline_df.dropDuplicates(['paperid'])
            z_len = country_papers_discipline_df.count()
            logger.info("Removed " + str(y_len - z_len) + " duplicates for " + country_name + " papers in discipline " + discipline)


            # Write the current dataset to file
            output_filename = join(cfg['hdfs']['onmerrit_dir'], country_name + "_" + discipline + "_papers.csv")
            country_papers_discipline_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
            logger.info("\n\nWrote dataset for "+ discipline + " discipline for  country : " + country_name + " to file " + output_filename + "\n\n")


    # done =================================================================== #