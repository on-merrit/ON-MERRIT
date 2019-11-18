"""
Utilities for logging
"""

import os
import json
import logging
from typing import NewType
from logging import handlers
from logging.config import dictConfig

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


LogLevel = NewType('LogLevel', int)


class MakeFileHandler(handlers.TimedRotatingFileHandler):
    """
    Creates the log directory in case it doesn't exist.
    """

    def __init__(
            self, filename, when='h', interval=1, backupCount=0, encoding=None,
            delay=False, utc=False, atTime=None
    ):
        """
        :param filename:
        :param when:
        :param interval:
        :param backupCount:
        :param encoding:
        :param delay:
        :param utc:
        :param atTime:
        """
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        super(MakeFileHandler, self).__init__(
            filename, when, interval, backupCount, encoding, delay, utc, atTime
        )


class LogUtils(object):
    """
    Utilities for logging.
    """

    @staticmethod
    def setup_logging(
            logging_config_path: str = 'logging.json', 
            logging_level: LogLevel = logging.DEBUG
    ) -> None:
        """
        Setup logging configuration
        :param logging_config_path: path to the logging configuration file
        :param logging_level: which message levels should be enabled
        :return: None
        """
        logging.basicConfig(level=logging_level)
        path = logging_config_path
        if os.path.exists(path):
            with open(path, 'rt') as f:
                logging_config = json.load(f)
            dictConfig(logging_config)
