
import logging

import pandas as pd
import json

from src.utils.config_loader import ConfigLoader
from os.path import join, abspath, dirname

from src.evaluation.experiments_logger import ExperimentsLogger


__author__ = 'bg3753'
__email__ = 'bikash.gyawali@open.ac.uk'

import gender_guesser.detector as gender



def create_author_gender_dataset() -> None:
    """Creates a csv mapping names of author to their gender (based on first name) as determined by the
     gender-guesser library -- https://pypi.org/project/gender-guesser/ .

     The input is a csv file with authornames (as extracted from ../spark/jobs/datasetextraction_question2).
     Output is a csv with the gender label.
    
    :return: The result will be one of unknown (name not found), andy (androgynous), male, female, mostly_male, or mostly_female.
    The difference between andy and unknown is that the former is found to have the same probability to be male than to be female, while the later means that the name wasn’t found in the database.
    :rtype: str
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Running {create_author_gender_dataset.__name__}')

    # setup
    logger.info('Loading config')
    app_cfg = ConfigLoader.load_config("config_gender_dataset.json")
    

    logger.info('Mapping Author names to their Gender')

    gen_detector = gender.Detector(case_sensitive=False)

    def get_gender(first_name):
        # print(first_name)
        try:
            result = gen_detector.get_gender(first_name)
            return result
        except:
            return "unknown"

    def get_gender_for_df_row(row):
        # Based on http://jonathansoma.com/lede/foundations/classes/pandas%20columns%20and%20functions/apply-a-function-to-every-row-in-a-pandas-dataframe/
        return get_gender(row['author_normalizedname'].split(" ")[0]) # Using the first name only.

    def load_df(file_path):
        # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
        author_names_df = pd.read_csv(file_path, header=0,
                                            sep=",", usecols = ["authorid","affiliationid","paperid","year","normalizedname","displayname","normalizedwikiname","author_normalizedname","author_displayname"],
                                            dtype={"authorid": object, "affiliationid": object, "paperid": object, "year": object, "wikipage": object, "normalizedwikiname": object})  # object means string
        # Then eliminate problematic lines
        #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
        author_names_df.drop(author_names_df[author_names_df.paperid == "paperid"].index, inplace=True)
        # Then reset dtypes as needed.
        author_names_df = author_names_df.astype({'year': int})
        return author_names_df




    for x in app_cfg['cname_in_file_names']:
        logger.info('Processing dataset for ' + x)
        author_name_filepath = join(app_cfg['paths']['data_dir'], 'author_names_'+x+'_papers.csv')
        author_names_df = load_df(author_name_filepath)

        author_names_df['gender'] = author_names_df.apply(get_gender_for_df_row, axis=1)

        # Write the output as csv
        out_filepath = join(app_cfg['paths']['output_dir'], 'author_gender_'+x+'_papers.csv')
        author_names_df.to_csv(out_filepath, sep=',', index=False, header=True)
        logger.info('Wrote author_gender dataset to '+out_filepath)


    # Saving config files for reproducibility
    el = ExperimentsLogger(app_cfg['paths']['output_dir'])
    model_cfg = ConfigLoader.load_config(app_cfg['model']['config'])
    result = {"result": "Wrote csv files to "+app_cfg['paths']['output_dir']}
    el.log_experiment(app_cfg, model_cfg, result)

    logger.info(f'Done running {create_author_gender_dataset.__name__}')
