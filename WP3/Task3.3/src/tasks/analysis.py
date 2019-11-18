
import logging

from src.utils.config_loader import ConfigLoader
from src.evaluation.results_parser import ResultsParser

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


def parse_results() -> None:
    """Parse all results into one results file
    
    :return: None
    :rtype: None
    """
    logger = logging.getLogger(__name__)
    logger.info(f'Running {parse_results.__name__}')

    # setup
    logger.info('Loading config')
    app_cfg = ConfigLoader.load_config()
    
    # parse results
    logger.info('Loading data')
    rp = ResultsParser(app_cfg['paths']['output_dir'])
    rp.parse_results()
    
    logger.info(f'Done running {parse_results.__name__}')
