
import os
import csv
import logging
from typing import Tuple, List, Dict

from sklearn.datasets import fetch_20newsgroups

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class DataLoader(object):

    def __init__(self, data_directory: str):
        """Initialise class
        
        :param data_directory: Project data directory. Should have the following
                               subdirectories: external, interim, processed, raw
        :type data_directory: str
        :return: None
        :rtype: None
        """
        self._logger = logging.getLogger(__name__)
        self._data_dir = data_directory

    def load_splits(
            self, splits_dir: str
    ) -> Dict[str, Tuple[List[str], List[int]]]:
        """Load splits for training/testing
        
        :param splits_directory: where to load splits from
        :type splits_directory: str
        :return: Dictionary with the following keys: train, dev, test. Each
                 value in the dictionary is a tuple of (data, labels)
        :rtype: Dict[str, Tuple[List[str], List[int]]]
        """
        splits = {}
        splits_path = os.path.join(self._data_dir, 'processed', splits_dir)
        for fname in os.listdir(splits_path):
            fpath = os.path.join(splits_path, fname)
            if os.path.isdir(fpath):
                continue
            with open(fpath) as fp:
                reader = csv.reader(fp)
                data, labels = zip(*[(row[2], row[1]) for row in reader])
                splits[fname.split('.')[0]] = (data, labels)
        return splits
