
import abc
from typing import Dict, Union, Iterable

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class BaseModel(object, metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def train_test(
        self, X_train: 'np.array[np.int64]', X_test: 'np.array[np.int64]', 
        y_train: Iterable[int], y_test: Iterable[int]
    ) -> Dict[str, Union[int, float]]:
        """Train a dummy classifier and return predictions on test data.

        :param X_train: numpy array of shape [n_train_samples, n_features]
        :type X_train: np.array[np.int64]
        :param X_test: numpy array of shape [n_test_samples, n_features]
        :type X_test: np.array[np.int64]
        :param y_train: numpy array of shape [n_train_samples]
        :type y_train: Iterable[int]
        :param y_test: numpy array of shape [n_test_samples]
        :type y_test: Iterable[int]
        :return: performance metrics
        :rtype: Dict[str, Union[int, float]]
        """
        raise NotImplementedError('Must define `train_test` to use this class')
