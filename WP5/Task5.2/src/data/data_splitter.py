""" Class for splitting data into train, dev, and test sets and for splitting
    for cross-validation
"""

import math
import logging

import numpy as np
from sklearn.model_selection import (
    ShuffleSplit, StratifiedShuffleSplit, StratifiedKFold, KFold
)

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class DataSplitter(object):

    def __init__(
            self, dev_ratio: float = 0.15, test_ratio: float = 0.2, 
            n_splits: int = None, random_state: int = None
    ):
        """ Initialize class. If n_splits is set, it will override test_ratio.
        
        :param dev_ratio: proportion of data to be withheld for model 
                          development, defaults to 0.15
        :type dev_ratio: float, optional
        :param test_ratio: proportion of data to be withheld for testing, 
                           defaults to 0.2
        :type test_ratio: float, optional
        :param n_splits: number of splits in case of splitting for 
                         cross-validation, defaults to None
        :type n_splits: int, optional
        :param random_state: the seed used by the random number generator, 
                             defaults to None
        :type random_state: int, optional
        """
        self._logger = logging.getLogger(__name__)
        self._dev_ratio = dev_ratio
        self._test_ratio = test_ratio if not n_splits else 1.0 / n_splits
        self._n_splits = n_splits
        self._random_state = random_state
        self._logger.info('Using random state: {}'.format(self._random_state))
        self._logger.info('Splits: {:.2f}/{:.2f}/{:.2f} train/dev/test'.format(
            1.0 - dev_ratio - test_ratio, dev_ratio, test_ratio
        ))

    def _shuffle_split(
            self, X: 'np.ndarray[Any]', y: 'np.ndarray[Any]', test_size: int, 
            stratified: bool = True
    ):
        """Yield indices to split data into training and test sets.
        
        :param X: matrix with training data
        :type X: [type]
        :param y: [description]
        :type y: [type]
        :param test_size: [description]
        :type test_size: int
        :param stratified: [description], defaults to True
        :type stratified: bool, optional
        :return: [description]
        :rtype: [type]
        """
        split_class = StratifiedShuffleSplit if stratified else ShuffleSplit
        split = split_class(
            n_splits=1,
            train_size=None,
            test_size=test_size,
            random_state=self._random_state
        )
        self._logger.info('Generating splits with {}'.format(
            split.__class__.__name__
        ))
        return next(split.split(X, y))

    def _k_fold_split(self, X, y, stratified=True):
        """
        :param X:
        :param y:
        :param n_splits:
        :param stratified:
        :return:
        """
        split_class = StratifiedKFold if stratified else KFold
        split = split_class(
            n_splits=self._n_splits,
            shuffle=True,
            random_state=self._random_state
        )
        self._logger.info('Generating splits with {}'.format(
            split.__class__.__name__
        ))
        return split.split(X, y)

    def split(self, X, y, stratified=True):
        """
        :param X:
        :param y:
        :param stratified:
        :return: dictionary {'indices_key': list[int]}
        """
        # prepare data
        X = np.array(X)
        y = np.array(y)
        n_samples = X.shape[0]
        i = np.arange(0, n_samples)
        self._logger.info('Samples: {}, labels size: {}, idx size: {}'.format(
            n_samples, y.shape, i.shape
        ))

        # how many samples should go into train, dev and test
        dev_size = math.ceil(n_samples * self._dev_ratio)
        test_size = math.ceil(n_samples * self._test_ratio)
        train_size = n_samples - dev_size - test_size
        self._logger.info('Train, dev, test sizes: {}/{}/{}'.format(
            train_size, dev_size, test_size
        ))

        # split data into train+dev and test
        train_dev_indices, test_indices = self._shuffle_split(
            X, y, test_size, stratified
        )
        self._logger.info('Train+dev size: {}, test size: {}'.format(
            train_dev_indices.shape, test_indices.shape
        ))

        # split train+dev into train and dev
        self._logger.info('Splitting train set into train and dev sets.')
        train_indices, dev_indices = self._shuffle_split(
            X[train_dev_indices], y[train_dev_indices], dev_size, stratified
        )

        # return indices
        ii = i[train_dev_indices]
        # returning a dictionary because it's easier to pass it around
        return {
            'train': ii[train_indices],
            'dev': ii[dev_indices],
            'test': i[test_indices]
        }

    def k_fold_split(self, X, y, stratified=True):
        """
        DataSplitter.splits has to be set for this to work
        :param X:
        :param y:
        :param stratified:
        :return: dictionary {'indices_key': list[int]}
        :raises: ValueError if DataSplitter.splits is not set
        """
        if self._n_splits is None:
            raise ValueError

        # prepare data
        X = np.array(X)
        y = np.array(y)
        n_samples = X.shape[0]
        i = np.arange(0, n_samples)
        self._logger.info('Samples: {}, labels size: {}, idx size: {}'.format(
            n_samples, y.shape, i.shape
        ))

        # how many samples should go into train, dev and test
        dev_size = math.ceil(n_samples * self._dev_ratio)
        test_size = math.ceil(n_samples * self._test_ratio)
        train_size = n_samples - dev_size - test_size
        self._logger.info('Train, dev, test sizes: {}/{}/{}'.format(
            train_size, dev_size, test_size
        ))

        # split data into train+dev and test
        for split in self._k_fold_split(X, y, stratified):
            train_dev_indices, test_indices = split[0], split[1]
            self._logger.info('Train+dev size: {}, test size: {}'.format(
                train_dev_indices.shape, test_indices.shape
            ))

            # split train+dev into train and dev
            self._logger.info('Splitting train set into train and dev sets.')
            train_indices, dev_indices = self._shuffle_split(
                X[train_dev_indices], y[train_dev_indices], dev_size, stratified
            )

            # return indices
            ii = i[train_dev_indices]
            # returning a dictionary because it's easier to pass it around
            yield {
                'train': ii[train_indices],
                'dev': ii[dev_indices],
                'test': i[test_indices]
            }
