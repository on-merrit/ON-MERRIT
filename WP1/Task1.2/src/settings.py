
def init(cfg_file: str = 'config.json'):
    """Initialize global variables that are shared across all project modules.
    
    :param cfg_file: name of config file in use, defaults to 'config.json'
    :type cfg_file: str, optional
    """
    global config_file
    config_file = cfg_file
