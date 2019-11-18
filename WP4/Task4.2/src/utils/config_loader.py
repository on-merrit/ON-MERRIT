
import json
from typing import Dict
from os.path import dirname, abspath, join, isabs

import src.settings

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class ConfigLoader(object):

    _project_root = dirname(dirname(dirname(abspath(__file__))))

    @staticmethod
    def load_config(config_path: str = None) -> Dict[str, Dict[str, str]]:
        if not config_path:
            # use default config
            config_path = join(
                ConfigLoader._project_root, src.settings.config_file
            )
        if not isabs(config_path):
            config_path = join(ConfigLoader._project_root, config_path)
        with open(config_path) as fp:
            return json.load(fp)
