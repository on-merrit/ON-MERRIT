import logging

import pandas as pd
import json
import ast

from src.utils.config_loader import ConfigLoader
from os.path import join, abspath, dirname

from src.evaluation.experiments_logger import ExperimentsLogger

from fp.fp import FreeProxy
from scholarly import scholarly

__author__ = 'bg3753'
__email__ = 'bikash.gyawali@open.ac.uk'


def set_new_proxy():
    while True:
        proxy = FreeProxy(rand=True, timeout=1).get()
        proxy_works = scholarly.use_proxy(http=proxy, https=proxy)
        if proxy_works:
            break
    # print("Working proxy:", proxy)
    return proxy


def get_sections_info(author):
    while True:
        try:
            current_author_profile = author.fill(sections=['basics',
                                                           'indices'])  # returns either bool or an Author object -- https://github.com/scholarly-python-package/scholarly/blob/master/scholarly/author.py#L132
            if current_author_profile:
                result = ast.literal_eval(repr(current_author_profile))
            else:
                result = "No info available."
            break
        except Exception as e:
            print("Trying new proxy for author fill")
            set_new_proxy()
    return result


# some more thing need to do to continously check proxy works during search_query and fill -- https://github.com/scholarly-python-package/scholarly
def get_author_profile(author_name):
    # print(author_name)
    all_profiles = {}
    while True:
        try:
            author_finder = scholarly.search_author(author_name)
            profile_counter = 0
            for author in author_finder:
                profile_counter = profile_counter + 1
                all_profiles["profile_" + str(profile_counter)] = get_sections_info(author)
            break
        except Exception as e:
            print("Trying new proxy for author search")
            set_new_proxy()
    return all_profiles


def extract_all_author_names(author_names_df):
    x = author_names_df.drop_duplicates(subset=['author_normalizedname'])
    all_author_names = x['author_normalizedname'].tolist()
    all_author_names.sort()
    return all_author_names


def load_df(file_path):
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    author_names_df = pd.read_csv(file_path, header=0,
                                  sep=",", usecols=["paperid", "author_normalizedname"],
                                  dtype={"paperid": object})  # object means string
    # Then eliminate problematic lines
    # temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    author_names_df.drop(author_names_df[author_names_df.paperid == "paperid"].index, inplace=True)
    # Drop the unnecessary column.
    author_names_df = author_names_df.drop("paperid", 1)
    return author_names_df


def create_author_GScholar_profile_dataset() -> None:
    """Creates a csv mapping names of author to Google Scholar Profile as extracted by the
     scholarly library -- https://github.com/scholarly-python-package/scholarly .

     The input is a csv file with authornames (as extracted from ../spark/jobs/datasetextraction_question3).
     Output is a csv with the GScholar author profile.
    
    There can be multiple profile matches for the same author name in Google Scholar, this script will record all
    such variations for the same author name.
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Running {create_author_GScholar_profile_dataset.__name__}')

    # setup
    logger.info('Loading config')
    app_cfg = ConfigLoader.load_config("config_gscholar_profile_dataset.json")

    logger.info('Mapping Author names to their Google Scholar Profile')

    set_new_proxy()  # Initialise the proxy

    for x in app_cfg['cname_in_file_names']:
        logger.info('Processing dataset for ' + x)
        print('\n\n\n\n\nProcessing dataset for ' + x + "\n\n\n")
        author_name_filepath = join(app_cfg['paths']['data_dir'], 'author_names_' + x + '_papers.csv')
        author_names_df = load_df(author_name_filepath)

        all_author_names = extract_all_author_names(author_names_df)
        count_author_names = len(all_author_names)
        all_author_profiles = {}
        for idx, author_name in enumerate(all_author_names):
            all_author_profiles[author_name] = {"matching_profiles": get_author_profile(author_name)}
            if idx % 100 == 0:
                print("\n\n\n\n\t\t\tCompleted processing " + str(
                    idx + 1) + " author profiles in " + x + " out of " + str(count_author_names) + "\n\n\n\n")

        # Write the output as json
        out_filepath = join(app_cfg['paths']['output_dir'], 'author_GScholar_profiles_' + x + '.json')
        with open(out_filepath, 'w', encoding='utf8') as json_file:
            json.dump(all_author_profiles, json_file, indent=4, sort_keys=True, ensure_ascii=False)
        logger.info('Wrote author GScholar profile info to ' + out_filepath)

    # Saving config files for reproducibility
    el = ExperimentsLogger(app_cfg['paths']['output_dir'])
    model_cfg = ConfigLoader.load_config(app_cfg['model']['config'])
    result = {"result": "Wrote json files to " + app_cfg['paths']['output_dir']}
    el.log_experiment(app_cfg, model_cfg, result)

    logger.info(f'Done running {create_author_GScholar_profile_dataset.__name__}')