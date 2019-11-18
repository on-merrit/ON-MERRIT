
import os
import random

import numpy as np

__author__ = 'dh8835'
__email__ = 'dasha.herrmannova@open.ac.uk'


class RandUtils(object):

    @staticmethod
    def set_random_seed(random_seed):
        """Set all random seeds to 'random_seed'. Sets the following seeds: OS,
        numpy, python random, torch (if installed).
        
        :param random_seed: [description]
        :type random_seed: [type]
        """
        os.environ['PYTHONHASHSEED'] = str(random_seed)
        np.random.seed(int(random_seed))
        random.seed(int(random_seed))
        try: 
            import torch
            torch.manual_seed(int(random_seed))
            if torch.cuda.is_available():
                torch.cuda.manual_seed_all(int(random_seed))
        except ImportError:
            pass
