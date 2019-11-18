
import json
import logging
import platform
import pkg_resources
from os.path import join
from typing import Dict, Any, List

from src.utils.file_utils import FileUtils

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class ExperimentsLogger(object):

    def __init__(self, output_dir: str) -> None:
        """Initialize class.
        
        :param output_dir: directory for storing results
        :type output_dir: str
        :return: none
        :rtype: None
        """
        self._logger = logging.getLogger(__name__)
        self._output_dir = output_dir

    def get_installed_libraries(self) -> List[str]:
        """Get a list of installed libraries with versions.
        
        :return: list of libraries and versions
        :rtype: List[str]
        """
        return sorted([
            f'{d.project_name}=={d.version}' 
            for d in iter(pkg_resources.working_set)
        ])

    def get_python_version(self) -> str:
        """Return Python version as string.
        
        :return: python version
        :rtype: str
        """
        return platform.python_version()

    def log_experiment(
            self, app_cfg: Dict[str, Any], model_cfg: Dict[str, Any], 
            results: Dict[str, Any]
    ) -> None:
        """[summary]

        :param results: [description]
        :type results: Dict[str, Any]
        """
        experiment_dir = join(self._output_dir, 'experiments')
        experiment_dir = FileUtils.create_timed_directory(
            experiment_dir, suffix=model_cfg['name']
        )
        with open(join(experiment_dir, 'app_config.json'), 'w') as fp:
            json.dump(app_cfg, fp, indent=4)
        with open(join(experiment_dir, 'model_config.json'), 'w') as fp:
            json.dump(model_cfg, fp, indent=4)
        with open(join(experiment_dir, 'results.json'), 'w') as fp:
            json.dump(results, fp, indent=4)
        with open(join(experiment_dir, 'requirements.txt'), 'w') as fp:
            fp.write('\n'.join(self.get_installed_libraries()))
        with open(join(experiment_dir, 'system.json'), 'w') as fp:
            json.dump({'python': self.get_python_version()}, fp, indent=4)
