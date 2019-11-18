
import logging
from os.path import join

import numpy as np

from src.utils.rand_utils import RandUtils
from src.data.data_loader import DataLoader
from src.utils.config_loader import ConfigLoader
from src.data.data_preprocessor import DataPreprocessor
from src.features.text_vectorizer import TextVectorizer
from src.models.dummy import Dummy
from src.models.lr import LR
from src.evaluation.experiments_logger import ExperimentsLogger

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


def classify() -> None:
    """Run classification of the 20 newsgroups dataset
    
    :return: None
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Running {classify.__name__}')

    # setup
    logger.info('Loading config')
    app_cfg = ConfigLoader.load_config()
    model_cfg = ConfigLoader.load_config(app_cfg['model']['config'])
    logger.info(f"Using random seed: {model_cfg['random_seed']}")
    RandUtils.set_random_seed(model_cfg['random_seed'])

    # load data
    logger.info('Loading data')
    dl = DataLoader(app_cfg['paths']['data_dir'])
    splits = dl.load_splits(app_cfg['model']['data'])

    # extract features
    logger.info('Extracting features')
    logger.info(f"Vectorizer class: {app_cfg['model']['vectorizer_class']}")
    vectorizer = eval(app_cfg['model']['vectorizer_class'])(
        model_cfg['tokenizer'], model_cfg['vectorizer']
    )
    X_train, X_test = vectorizer.vectorize(
        splits['train'][0] + splits['dev'][0], splits['test'][0]
    )
    y_train = np.array(splits['train'][1] + splits['dev'][1])
    y_test = np.array(splits['test'][1])

    # run classification
    logger.info('Running model')
    logger.info(f"Model class: {app_cfg['model']['model_class']}")
    model = eval(app_cfg['model']['model_class'])(model_cfg['model'])
    results = model.train_test(X_train, X_test, y_train, y_test)

    # log results
    logger.info('Logging results')
    el = ExperimentsLogger(app_cfg['paths']['output_dir'])
    el.log_experiment(app_cfg, model_cfg, results)
    logger.info(f'Done running {classify.__name__}')
