"""Utilities for working with files (e.g. creation of dated folders)
"""

import os
from datetime import datetime

class FileUtils(object):

    @staticmethod
    def ensure_dir(dir_path: str) -> None:
        """Create directory (including parent directories) if it does not exist.
        
        :param dir_path: directory to create
        :type dir_path: str
        :return: None
        :rtype: None
        """
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    @staticmethod
    def create_dated_directory(parent_directory: str) -> str:
        """Create a new directory inside parent_directory which uses today's
        date its name. Date format: %Y%m%d
        
        :param parent_directory: where to create the directory
        :type parent_directory: str
        :return: path to the created directory
        :rtype: str
        """
        today = datetime.today()
        dir_path = os.path.join(parent_directory, today.strftime('%Y%m%d'))
        FileUtils.ensure_dir(dir_path)
        return dir_path

    @staticmethod
    def create_timed_directory(
            parent_directory: str, suffix: str = None
    ) -> str:
        """Create a new directory inside parent_directory which uses
           date & time as its name. Date format: %Y%m%d%H%m

        :param parent_directory: where to create the directory
        :type parent_directory: str
        :param suffix: optional suffix for the name, default: None
        :type suffix: str, optional
        :return: path to the created directory
        :rtype: str
        """
        today = datetime.now()
        dir_name = (
            f"{today.strftime('%Y%m%d%H%M')}_{suffix}"
            if suffix else today.strftime('%Y%m%d%H%M')
        )
        dir_path = os.path.join(parent_directory, dir_name)
        FileUtils.ensure_dir(dir_path)
        return dir_path

