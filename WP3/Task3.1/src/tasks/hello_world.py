import logging

from src.utils.rand_utils import RandUtils
from src.utils.config_loader import ConfigLoader

from src.evaluation.experiments_logger import ExperimentsLogger


def hello_world():
    logger = logging.getLogger(__name__)
    logger.info('Running {}'.format(hello_world.__name__))

    app_cfg = ConfigLoader.load_config()
    random_seed = app_cfg['preprocessing']['random_seed']
    RandUtils.set_random_seed(random_seed)


    print("Hello World!!")


    model_cfg = ConfigLoader.load_config(app_cfg['model']['config'])
    # Todo : run the model
    result={"Hello":"World"}  # sample result from running the model.
    
    el = ExperimentsLogger(app_cfg['paths']['output_dir'])
    el.log_experiment(app_cfg, model_cfg, result)


    logger.info('Done')