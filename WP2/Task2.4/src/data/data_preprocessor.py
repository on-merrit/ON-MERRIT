""" Runs data processing scripts to turn raw data from (data/raw) into
    cleaned data ready to be analyzed (saved in data/processed).
"""

import csv
import logging
from os.path import join, abspath, dirname

import numpy as np
from sklearn.datasets import fetch_20newsgroups

from src.utils.file_utils import FileUtils
from src.data.data_splitter import DataSplitter

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class DataPreprocessor(object):

    _20ng_selected_categories = ['alt.atheism', 'talk.religion.misc']

    def __init__(self, data_dir: str) -> None:
        """Initialize class
        
        :param data_dir: Main data directory, should have the following 
                         subdirs: external, interim, processed, raw
        :type data_dir: str
        :return: None
        :rtype: None
        """
        self._logger = logging.getLogger(__name__)
        self._data_dir = data_dir

    def _contains_alpha(self, t: str) -> bool:
        """Check if string contains at least one letter
        
        :param t: input string
        :type t: str
        :return: true if 't' contains at least one letter, false otherwise
        :rtype: bool
        """
        return any(c.isalpha() for c in t)

    def download_dataset(self) -> str:
        """Download & clean dataset and store a version ready to be analyzed in
        data/processed.
        
        :return: path to the dataset file
        :rtype: None
        """
        self._logger.info('Making final dataset from raw data')
        raw_dir = join(self._data_dir, 'raw')
        processed_dir = join(self._data_dir, 'processed')
        self._logger.info(
            f'Downloading 20 newsgroups dataset, selected classes: '
            f'{DataPreprocessor._20ng_selected_categories}'
        )
        # download
        data = fetch_20newsgroups(
            subset='train', data_home=raw_dir, 
            categories=DataPreprocessor._20ng_selected_categories,
            remove=('headers','footers','quotes')
        )
        self._logger.info(f'Got {len(data.data)} data points')
        output_dir = FileUtils.create_dated_directory(processed_dir)
        output_path = join(output_dir, '20ng_5cls.csv')
        self._logger.info(f'Storing cleaned dataset in {output_path}')
        # save and clean
        with open(output_path, 'w') as fp:
            writer = csv.writer(fp)
            writer.writerow(['idx', 'label', 'text'])
            good_rows = 0
            for idx, txt in enumerate(data.data):
                # remove rows which don't contain a single letter
                if not self._contains_alpha(txt):
                    continue
                writer.writerow([idx, data.target_names[data.target[idx]], txt])
                good_rows += 1
        self._logger.info(f'Size of clean dataset: {good_rows}')
        return output_path

    def split_dataset(
            self, dataset_path: str, random_state: int = None
    ) -> None:
        """Loads dataset in dataset_path and splits it into train/test
        subsets.
        
        :param dataset_path: Path to CSV with data to use. Splits will be stored 
                             in the same directory as the dataset.
        :type dataset_path: str
        :param random_state: [description], defaults to None
        :type random_state: int, optional
        :return: None
        :rtype: None
        """
        self._logger.info('Splitting data into train/dev/test')
        with open(dataset_path) as fp:
            reader = csv.reader(fp)
            next(reader) # skip first line with header
            idx, X, y = zip(*[(row[0], row[2], row[1]) for row in reader])
            label_names = list(set(y))
            y = np.array([label_names.index(x) for x in y])
            X = np.array(X)
        self._logger.info(f'Got {len(X)} samples')
        ds = DataSplitter(random_state=random_state)
        splits = ds.split(X, y)
        self._logger.info(
            f"Split size: train {len(splits['train'])}, "
            f"dev {len(splits['dev'])}, test {len(splits['test'])}"
        )
        splits_directory = join(
            dirname(abspath(dataset_path)), 'train_dev_test_split'
        )
        FileUtils.ensure_dir(splits_directory)
        self._logger.info(f'Will store splits in {splits_directory}')
        for k, v in splits.items():
            with open(join(splits_directory, f'{k}.csv'), 'w') as fp:
                writer = csv.writer(fp)
                for x in v:
                    writer.writerow([idx[x], y[x], X[x]])
        self._logger.info('Done splitting data')
        return splits_directory
        