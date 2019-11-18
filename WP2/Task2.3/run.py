"""
Main file for running the app. Use
python run.py       to print the app menu or
python run.py -h    to print help
"""

import sys
import logging
import argparse
from typing import List

import src.settings
from src.utils.log_utils import LogUtils
from src.tasks.preprocessing import prepare_dataset
from src.tasks.experiments import classify
from src.tasks.analysis import parse_results

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


def exit_app() -> None:
    """
    Exit app.

    :return: None
    :rtype: None
    """
    logging.getLogger(__name__).info('Exiting application')
    exit()


def menu() -> None:
    """
    Print app menu.
    
    :return: None
    :rtype: None
    """
    print('\nPossible actions:')
    print('=' * len('Possible actions:'))
    for k, v in sorted(menu_actions.items()):
        print('{}: {}'.format(k, v.__name__))
    print('Please select option(s):')
    actions = [i.lower() for i in list(sys.stdin.readline().strip())]
    exec_actions(actions)


def exec_actions(actions: List[str]) -> None:
    """
    Execute selected actions.

    :param actions: list of actions to be executed
    :type actions: List[str]
    :return: None
    :rtype: None
    """
    if not actions:
        exit_app()
    else:
        options = [
            (k, menu_actions[k].__name__) 
            if k in menu_actions else (k, 'invalid action') 
            for k in actions
        ]
        print('\nSelected the following options: \n{}'.format(options))
        for action in actions:
            if action in menu_actions:
                menu_actions[action]()
    menu()


menu_actions = {
    '0': prepare_dataset,
    '1': classify,
    '2': parse_results,
    'x': exit_app,
    'm': menu
}


if __name__ == '__main__':
    LogUtils.setup_logging()

    # parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config', metavar='confif_file', type=str,
        help='which config file to use'
    )
    parser.add_argument(
        '-a', '--actions', metavar='N', type=str, nargs='+',
        help=(
            'actions to be executed'
        )
    )
    cli_args = parser.parse_args()

    # save config file name
    src.settings.init(
        cfg_file=cli_args.config if cli_args.config else 'config.json'
    )

    # setup logger
    logger = logging.getLogger(__name__)
    logger.info('Application started')
    logger.info('Using %s config file', src.settings.config_file)

    # either display menu or directly execute actions
    if cli_args.actions:
        # append action to exit app after all actions were executed
        if cli_args.actions[-1] != 'x':
            cli_args.actions.append('x')
        exec_actions(cli_args.actions)
    else:
        menu()
