
import os
import json
import logging
from os.path import join

import pandas as pd


__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class ResultsParser(object):

    def __init__(self, output_dir: str) -> None:
        """Initialize class.
        
        :param output_dir: directory for storing results
        :type output_dir: str
        :return: none
        :rtype: None
        """
        self._logger = logging.getLogger(__name__)
        self._output_dir = output_dir

    def parse_results(self) -> None:
        """Parse all results files and put all results in one CSV file.
        
        :return: None
        :rtype: None
        """     
        experiment_dir = join(self._output_dir, 'experiments')
        all_results = []
        self._logger.info('Parsing all results in %s', experiment_dir)
        for dir_name in os.listdir(experiment_dir):
            dir_path = join(experiment_dir, dir_name)
            if os.path.isdir(dir_path):
                res_path = join(dir_path, 'results.json')
                cfg_path = join(dir_path, 'model_config.json')
                if os.path.exists(res_path) and os.path.exists(cfg_path):
                    with open(res_path) as fp:
                        results = json.load(fp)
                    with open(cfg_path) as fp:
                        cfg = json.load(fp)
                        results['name'] = cfg['name']
                        results['description'] = cfg['description']
                        results['dir'] = dir_name
                    all_results.append(results)
        self._logger.info('Found %d experiments', len(all_results))
        output_file = join(self._output_dir, 'results.csv')
        self._logger.info('Storing all results in %s', output_file)
        pd.DataFrame(all_results).to_csv(output_file)
