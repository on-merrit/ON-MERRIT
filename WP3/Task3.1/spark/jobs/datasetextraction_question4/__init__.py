'''
Q4: Distribution of seniority of the staff (number of years since their first publication until the last publication in the given university) at a particular university.

Extracts dataset of papers for authors with their publication year  for all the universities listed in THE WUR.

A separate csv will be generated for each country with records of all authors of all THE WUR universities in that country.

Important considerations about this work :
1. MAG can generate duplicate entries for papers published by any university because the same paperid could belong to different authors from the same university. This dataset WILL contain such duplicate paperids(authorship) from the same university but with different author ids.  Papers with authorship from multiple universities are counted once towards each of the universities concerned.
2. When there are multiple collaborators(authors) for the same paper, this dataset makes sure that only the records for collaborators from within selected universities are preserved.
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
    # MAIN FUNCTION -- name of the task is datasetextraction_question4  #
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
    logger.info('Extracting dataset of author publication years for all papers published by all THE WUR universities in the countries of our choice.')

    # MAG dataset to use
    db_name = cfg['mag_db_name']


    # tables that will be used
    papers_df = ss.table(db_name + '.papers').select(['paperid', 'year']).drop_duplicates()  # this will be used to extract publication year for papers.
    paper_author_affiliation_df = ss.table(db_name + '.paperauthoraffiliations').select(['paperid', 'authorid', 'affiliationid']).drop_duplicates()  # this will be used to extract authorid for papers -- A paper may have multiple authors but their authorid will be distinct.
    aff_df = ss.table(db_name + '.affiliations').select('affiliationid', 'normalizedname', 'displayname', 'wikipage')  # this will be used to identify institutions names
    # Based on the authorid (from paper_author_affiliation_df) and the year of publication (from papers_df), the analysis script will determine the seniority of each author within the university.


    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():

        country_papers_df = ss.read.csv(join(cfg['hdfs']['onmerrit_dir'], country_name+'_papers.csv'), header=True, mode="DROPMALFORMED")
        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        country_papers_df = country_papers_df.withColumn('normalizedwikiname', mag_normalisation_wiki_link_udf(country_papers_df["wikipage"]))

        country_papers_author_names_df = None

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

            # We will retain only the paperid from the univ_papers_df because the country_papers_df from which it was
            # extracted contains part info from paperauthoraffiliations table and in a different manner, for example,
            # without containing multiple entries for author names from the same university. We used the country_papers_df
            # to obtain the unique paperids that belong to our universities of choice but now, we will make a new join
            # with the paperauthoraffiliations table to get the data in the expected manner.
            univ_papers_df = univ_papers_df.select('paperid')

            # Get publication year for each of the papers in the dataset.
            univ_papers_df = univ_papers_df.join(papers_df, ['paperid'], how='inner')

            # Get the author ids along with their affiliation ids for the paperids
            univ_papers_authorid_df = univ_papers_df.join(paper_author_affiliation_df, ['paperid'], how='inner')
            # Get the name of the institutions
            univ_papers_authorid_df = univ_papers_authorid_df.join(aff_df, ['affiliationid'], how='inner')  # inner join

            # We will need to further ensure that the collaborator authors are from the same university only. Similar practice as above
            univ_papers_authorid_df = univ_papers_authorid_df.withColumn('normalizedwikiname', mag_normalisation_wiki_link_udf(univ_papers_authorid_df["wikipage"]))
            univ_papers_authorid_df_set1 = univ_papers_authorid_df[univ_papers_authorid_df['normalizedname'] == univ_name_normalised]
            univ_papers_authorid_df_set2 = univ_papers_authorid_df[univ_papers_authorid_df['normalizedwikiname'] == univ_name_normalised]
            univ_papers_authorid_df = univ_papers_authorid_df_set1.union(univ_papers_authorid_df_set2)
            univ_papers_authorid_df = univ_papers_authorid_df.dropDuplicates()


            # Update the total results for the country
            if country_papers_author_names_df is None and len(univ_papers_authorid_df.head(1)) > 0:
                country_papers_author_names_df = univ_papers_authorid_df
            else:
                # https://datascience.stackexchange.com/a/27231
                if len(univ_papers_authorid_df.head(1)) > 0:
                    country_papers_author_names_df = country_papers_author_names_df.union(univ_papers_authorid_df.select(country_papers_author_names_df.columns))

            logger.info("\nExtracted dataset for the university: " + univ_name)

        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "author_ids_" + country_name + "_papers.csv" )
        country_papers_author_names_df.write.csv(output_filename, mode="overwrite", header=True, sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country: " + country_name + " to file " + output_filename + "\n\n")




    logger.info("\n\n\nTask Complete.")
    # done =================================================================== #