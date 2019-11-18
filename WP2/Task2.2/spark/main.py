
import os
import sys
import time
import json
import logging
import argparse
import importlib

import pyspark


if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')


def setup_logging():
    """
    Set parameters for logging into stdout.
    :return: None
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_config():
    """
    Load and return app config from config.json.example file.
    :return:
    """
    if os.path.exists('config.json'):
        sys.path.insert(0, 'config.json')
    else:
        sys.path.insert(0, '../config.json')
    with open('config.json') as fp:
        return json.load(fp)


if __name__ == '__main__':
    setup_logging()
    cfg = load_config()
    logger = logging.getLogger(__name__)
    logger.info('Application started')

    arg_parser = argparse.ArgumentParser(
        description='Run a PySpark job'
    )
    arg_parser.add_argument(
        '--job', type=str, required=True, dest='job_name', 
        help='The name of the job module you want to run.'
    )
    args = arg_parser.parse_args()

    logger.info('Running job: {}'.format(args.job_name))
    sc = pyspark.SparkContext(appName=args.job_name)
    logger.info('Spark context created, Spark version: {}'.format(sc.version))
    job_module = importlib.import_module('jobs.{}'.format(args.job_name))

    start_time = time.time()
    job_module.analyze(sc, cfg)
    end_time = time.time()

    logger.info('Execution of job {} took {} seconds'.format(
        args.job_name, end_time - start_time
    ))
    logger.info('Application completed')
