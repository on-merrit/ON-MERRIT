
import logging
from typing import Dict, Union, Iterable, Any

import numpy as np
from sklearn.linear_model.logistic import LogisticRegression

from src.models.base_model import BaseModel
from src.evaluation.metrics import Metrics

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class LR(LogisticRegression, BaseModel):

    def __init__(self, model_cfg: Dict[str, Any] = None) -> None:
        """Initialize class, pass config to parent
        
        :param model_cfg: model config
        :type model_cfg: Dict[str, Any], default None
        :return: None
        :rtype: None
        """
        if not model_cfg: 
            model_cfg = {}
        super(LR, self).__init__(**model_cfg)
        self._logger = logging.getLogger(__name__)

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
        y_train = np.array(y_train)
        y_test = np.array(y_test)

        # fit
        self._logger.info(f'Fitting {__name__} classifier to data')
        self.fit(X_train, y_train)

        # predict
        self._logger.info('Done fitting to data, obtaining predictions')
        pred_train = self.predict(X_train)
        pred_test = self.predict(X_test)
        results = {
            f'train_{k}': v for k, v in
            Metrics.metrics(pred_train, y_train).items()
        }
        results.update({
            f'test_{k}': v for k, v in
            Metrics.metrics(pred_test, y_test).items()
        })
        self._logger.info(f'Done testing {__name__}')
        return results
