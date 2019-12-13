#!/usr/bin/env python
# coding: utf-8

# Q. 2: How are papers from our selected institutions distributed across the three different scientific disciplines (agriculture, climatology and medicine) we chose?

# In[1]:


# standard path wrangling to be able to import project config and sources
import os
import sys
from os.path import join
root = os.path.dirname(os.getcwd())
sys.path.append(root)
print('Project root: {}'.format(root))


# In[2]:


get_ipython().system('ls $root')


# In[3]:


# Built-in
import json

# Installed
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

import unicodedata
import re


# In[4]:


def mag_normalisation_institution_names(institution_name):
        '''
        An approximation function to estimate the way MAG normalises university names
        :param institution_name: The input name of the institution to normalise as per MAG
        :type str
        :return: normalised names as in the field "normalizedname" of "affiliations" table in MAG.
        :type str
        '''

        # Replace non-ascii by ascii. See https://stackoverflow.com/a/3704793/530399
        # norm_uname = unicodedata.normalize('NFKD', university_name).encode('ascii', 'ignore')
        # https://stackoverflow.com/a/14785625/530399 Python 3 replaced unicode with str
        norm_uname = unicodedata.normalize('NFKD', institution_name).encode("ascii", "ignore").decode("ascii")
        norm_uname = norm_uname.lower()
        # Only preserve the a-z characters and replace the rest by space
        norm_uname = re.sub(r'[^\x61-\x7A]+',' ', norm_uname)
        norm_uname = norm_uname.strip()

        return norm_uname


# In[5]:


cfg = None
with open(join(root,"spark/config.json")) as fp:
    cfg = json.load(fp)


# In[6]:


# cfg


# In[7]:


# Create a new directory to save results
output_dir = join(root,"documents/analysis/dataset_selection_question2")
os.makedirs(output_dir)


# In[8]:


def get_institutions_paper_count_plot(selected_institutions_papercount_df, paper_count_df, country_name, discipline, output_dir):
    
    all_insts = paper_count_df['normalizedname'].values.tolist()
    all_inst_paper_counts = paper_count_df['paper_counts'].values.tolist()
    
    selected_insts = selected_institutions_papercount_df['normalizedname'].values.tolist()
    selected_inst_paper_counts = selected_institutions_papercount_df['paper_counts'].values.tolist()
    
    
    plt.figure(figsize=(15,10))
    
    
    
    
    plt.plot(all_insts, all_inst_paper_counts, color='blue')
    plt.scatter(selected_insts, selected_inst_paper_counts, color='red') # overlay of scatter plot for selected institutions
    
    ax = plt.gca()
    
    # Annotate the name of the selected universities to the plot
    for i, txt in enumerate(selected_insts):
        ax.annotate(txt, (selected_insts[i], selected_inst_paper_counts[i]))
    
    
    ax.set_xlabel("Institutions in "+country_name)
    ax.set_ylabel("Publication count in "+discipline+ " domain")
    

    # https://stackoverflow.com/a/12998531/530399
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False)
    
    plt.savefig(join(output_dir,country_name+"_"+discipline+'.png'), bbox_inches='tight')
    plt.savefig(join(output_dir,country_name+"_"+discipline+'.pdf'), bbox_inches='tight')
    
    plt.close()
    
    return ax.get_figure()


# In[9]:


all_countries_plot = {}

for discipline in cfg['data']['fields_of_study']:

    all_countries_plot[discipline] = {}
    
    for country_name,institutions_name in cfg['data']['institutions_by_country'].items():

        country_discipline_df = pd.read_csv(join(root,"data/processed/"+country_name+"_"+discipline+"_papers.csv"), header=0, sep=",")
        
        #  temp fix until spark csv merge header issue is resolved
        country_discipline_df.drop(country_discipline_df[country_discipline_df.paperid == "paperid"].index, inplace=True)
    
        
        paper_count_df = country_discipline_df.groupby('normalizedname').count()[['paperid']].rename(columns={'paperid': 'paper_counts'}).reset_index()  # dataframe of count of papers in current discipline for all institutions in current country. The index of this dataframe is institution normalizedname and has a single column named papers_count
        
        paper_count_df = paper_count_df.sort_values('paper_counts', ascending=True)
        
        norm_institute_names=[mag_normalisation_institution_names(x) for x in institutions_name]
        selected_institutions_papercount_df = paper_count_df[paper_count_df['normalizedname'].isin(norm_institute_names)]
        
        
        plt_country_institutions = get_institutions_paper_count_plot(selected_institutions_papercount_df, paper_count_df, country_name, discipline, output_dir)
        
        all_countries_plot[discipline][country_name] = plt_country_institutions
        
        
        
        print("Saved plot for dataset of "+discipline+" in "+country_name+"\n")


# In[ ]:





# In[ ]:





# In[10]:


all_countries_plot["climatology"]["netherlands"]


# In[11]:


all_countries_plot["agriculture"]["germany"]


# In[ ]:




