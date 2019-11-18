
import logging

from src.utils.rand_utils import RandUtils
from src.utils.config_loader import ConfigLoader
from src.data.data_preprocessor import DataPreprocessor

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


def prepare_dataset() -> None:
    """Download dataset and split into training/dev/test data for experiments
    
    :return: None
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.info('Running {}'.format(prepare_dataset.__name__))

    cfg = ConfigLoader.load_config()
    random_seed = cfg['preprocessing']['random_seed']
    RandUtils.set_random_seed(random_seed)

    logger.info('Downloading dataset and preparing splits')
    dp = DataPreprocessor(cfg['paths']['data_dir'])
    dataset_path = dp.download_dataset()
    dp.split_dataset(dataset_path)
    logger.info('Done')
