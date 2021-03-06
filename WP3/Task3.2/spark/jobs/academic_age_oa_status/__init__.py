"""
This script merges the datasets on author ids and OA status of papers.
"""


import sys
import logging
from os.path import join


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

    # MAG dataset to use
    db_name = cfg['mag_db_name']

    papers_df = ss \
        .table(db_name + '.papers') \
        .select(['paperid', 'year']) \
        .drop_duplicates()
    paper_author_affiliation_df = ss \
        .table(db_name + '.paperauthoraffiliations') \
        .select(['paperid', 'authorid', 'affiliationid']) \
        .drop_duplicates()

    for country_name, univ_names in cfg['data']['all_THE_WUR_institutions_by_country'].items():
        # for country_name in ['austria']:

        logger.info("\n\n\nProcessing dataset of papers from " + country_name)

        logger.info('Reading files.')
        # get papers including oa status
        country_papers_oa_df = ss.read.csv(
            join(cfg['hdfs']['onmerrit_dir'],
                 'oa_status_'+country_name+'_papers.csv'),
            header=True, mode="DROPMALFORMED")

        # select relevant columns
        country_papers_oa_df = country_papers_oa_df \
            .select('paperid', 'affiliationid', 'normalizedname', 'displayname',
                    'is_OA', 'year')

        # get authors for selected institutions in THE ranking
        country_authors_df = ss.read.csv(
            join(cfg['hdfs']['onmerrit_dir'],
                 'author_names_' + country_name + '_papers.csv'),
            header=True, mode="DROPMALFORMED")

        # select relevant columns
        country_authors_df = country_authors_df. \
            select('authorid', 'paperid', 'author_normalizedname',
                   'author_displayname')

        # merge papers with authors
        country_merged = country_papers_oa_df \
            .join(country_authors_df, ['paperid'], how='left')

        # get papers and author/paper dataset to calculate first paper (year)
        logger.info('Calculate year of first paper.')

        # only look at our authors
        # need to drop duplicates since we are taking all papers of the authors
        # otherwise this results in many duplicates per individual
        selected_authors = country_merged.select(['authorid']).drop_duplicates()

        # find all papers from our authors
        all_paper_ids = selected_authors \
            .join(paper_author_affiliation_df, ['authorid'], how='left')

        # find the full papers including year information
        all_papers = all_paper_ids.join(papers_df, ['paperid'], how='left')

        # select relevant cols
        all_papers = all_papers.select(['authorid', 'paperid', 'year'])

        # we want to find the first year (i.e. paper) per author
        # get min year per author
        first_papers = all_papers.groupby('authorid').min('year')

        # rejoin with all papers, since .groupby drops the paperid
        first_papers = all_papers \
            .join(first_papers, ['authorid'], how='left') \
            .withColumnRenamed('min(year)', 'first_paper') \
            .select(['authorid', 'paperid', 'first_paper'])

        country_merged_with_year = country_merged. \
            join(first_papers, ['paperid', 'authorid'], how='left')

        # all the above processes somehow introduce many duplicates
        country_merged_with_year = country_merged_with_year.drop_duplicates()

        # todo: calculate average oa rate per age group

        logger.info('Write to file (' + country_name + ').')
        # save the data for the current country
        output_filename = join(cfg['hdfs']['onmerrit_dir'], "oa_and_authors_" +
                               country_name + ".csv")

        country_merged_with_year. \
            write.csv(output_filename, mode="overwrite", header=True,
                      sep=",", quoteAll=True)
        logger.info("\n\nWrote dataset for country : " + country_name +
                    " to file " + output_filename + "\n\n")

    logger.info('Done.')
