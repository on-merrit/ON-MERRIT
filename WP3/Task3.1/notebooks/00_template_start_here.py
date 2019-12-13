#!/usr/bin/env python
# coding: utf-8

# # Notebook template
# Copy this template when starting a new notebook. This template contains:
# * path wrangling (to be able to import scripts, data, etc. from project root)
# * matplotlib & pandas setup
# * config loading

# In[ ]:


# standard path wrangling to be able to import project config and sources
import os
import sys
from os.path import join
root = os.path.dirname(os.getcwd())
sys.path.append(root)
print('Project root: {}'.format(root))


# Imports:

# In[ ]:


# Built-in
import json

# Installed
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

# Custom
from src.utils.config_loader import ConfigLoader


# Load config:

# In[ ]:


cfg = ConfigLoader().load_config()
print('Config loaded.')


# Other setup:

# In[ ]:


sns.set()
palette = sns.color_palette('muted')
sns.set_palette(palette)

pd.options.display.float_format = '{:.2f}'.format
pd.set_option('max_colwidth', 800)

