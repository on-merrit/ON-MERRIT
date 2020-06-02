#!/usr/bin/env python
# coding: utf-8

# 
# # This will create plots for institutions of universities in THE WUR univs only and for the period of 2007-2017. The input dataset contains info of THE WUR univs only but for any period of time.

# #### This is to be compatible with other analysis questions which used dataset from the period of 2007 to 2017

# ## Question : Distribution of seniority of the staff (number of years since their first publication until the last publication in the given university) at a particular university.

# In[1]:


# standard path wrangling to be able to import project config and sources
import os
import sys
from os.path import join
root = os.path.dirname(os.getcwd())
sys.path.append(root)
print('Project root: {}'.format(root))


# In[2]:


sys.path.append(join(root,"spark/shared/"))
from MAG_utils import *


# In[ ]:





# In[3]:


# Built-in
import json

# Installed
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib import rc,rcParams
from matplotlib.patches import Rectangle
import swifter

import unicodedata
import re
from statistics import mean

import ast


# In[4]:


cfg = None
with open(join(root,"spark/config.json")) as fp:
    cfg = json.load(fp)


# In[5]:


# cfg


# In[6]:


cnames_for_plot = {
    "austria" : "Austria",
    "brazil" : "Brazil",
    "germany" : "Germany",
    "india" : "India",
    "portugal" : "Portugal",
    "russia" : "Russia",
    "uk" : "UK",
    "usa" : "USA"
}


# In[7]:


output_dir = join(root,"documents/analysis/dataset_selection_question4")


# In[ ]:


# Create a new directory to save results
os.makedirs(output_dir)


# In[8]:


study_years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]


# In[ ]:





# In[ ]:





# In[ ]:





# # Extraction of count of papers in each of the three gender categories for  publications coming from each university.

# In[9]:


def get_average_seniority_years(univ_papers_df):
    unique_author_ids = univ_papers_df.authorid.dropna().unique().tolist()
    total_seniority_years = 0
    
#     print(len(unique_author_ids))
    
    for author_id in unique_author_ids:
        author_pubs = univ_papers_df[univ_papers_df["authorid"] == author_id].dropna(subset=['year'])
        
        first_pub_year = author_pubs['year'].min()
        latest_pub_year = author_pubs['year'].max()
        seniority_years = latest_pub_year - first_pub_year + 1
        
        total_seniority_years = total_seniority_years + seniority_years
        
    return total_seniority_years/len(unique_author_ids)


# In[10]:


def get_univ_authors_seniority_counts(country_papers_fos_df, univs_name):    
    '''
    Get the plot of count of papers in each discipline from each university in the input country.
    '''
    univs_info = {}
    
    univs_not_found = []
    univs_found = []
    
    for org_univ_name in set(univs_name):  # remove duplicate univ names in the THE list, if any
#         print(org_univ_name)

        THE_univ_name_normalised = mag_normalisation_institution_names(org_univ_name)
    
        '''
        The dataframe that will be selected for the current univ is either :
        1. When the MAG normalizedname column matches to THE_univ_name_normalised
        or
        2. When the MAG normalised(wikiname) matches to THE_univ_name_normalised -- this matches English names (in MAG wiki links as well as THE) of non English name (in MAG normalisedname or displayname) universities.
        '''
        univ_papers_df_set1 = country_papers_fos_df[country_papers_fos_df['normalizedname']==THE_univ_name_normalised]
        
        univ_papers_df_set2 = country_papers_fos_df[country_papers_fos_df['normalizedwikiname']==THE_univ_name_normalised]
        
        # The records in two sets can be the exactly the same 
        # Concat and remove exact duplicates  -- https://stackoverflow.com/a/21317570/530399
        univ_papers_df = pd.concat([univ_papers_df_set1, univ_papers_df_set2]).drop_duplicates().reset_index(drop=True)

#         Put additional criteria that these papers are from 2007 till 2017
        univ_papers_df = univ_papers_df[univ_papers_df['year'].isin(study_years)]
        
        
        # For those I couldn't match/find their name, it is not fair to say that their count of papers in any discipline was 0. Should be excluded from the graph.
        if len(univ_papers_df)==0:
            univs_not_found.append(org_univ_name+"    @    "+THE_univ_name_normalised)
        else:
            univs_found.append(org_univ_name)
            
            
            # Count the number of seniority years for each distinct author in this dataset
            univ_avg_seniority_years = get_average_seniority_years(univ_papers_df)            
            
            univs_info[org_univ_name] = {}
            univs_info[org_univ_name]["avg_seniority_years"] = univ_avg_seniority_years
        
    return univs_info, univs_not_found, univs_found


# In[11]:


def load_dataset(country_name):
     # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_authors_df = pd.read_csv(join(root,"data/processed/author_ids_"+country_name+"_papers.csv"), header=0, sep=",", dtype={"year": object})  # object means string
    
        
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_authors_df.drop(country_papers_authors_df[country_papers_authors_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_authors_df = country_papers_authors_df.astype({'year':int})
    
    return country_papers_authors_df


# In[12]:


all_countries_all_univs_seniority_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    print("\nProcesing for dataset of univs in "+country_name+"\n")
    all_countries_univs_found_not_found[country_name] =  {}
    
    country_papers_authors_df = load_dataset(country_name)
    
    univs_info, univs_not_found, univs_found = get_univ_authors_seniority_counts(country_papers_authors_df, univs_name)
    
    all_countries_all_univs_seniority_info[country_name] = univs_info
    
    count_total_univs = len(univs_not_found) + len(univs_found)
    
    not_found_details = {}
    not_found_details['univ_names'] = univs_not_found
    not_found_details['count_univs'] = len(univs_not_found)
    not_found_details['percent_univs'] = (len(univs_not_found)*100.00)/count_total_univs
    
    found_details = {}
    found_details['univ_names'] = univs_found
    found_details['count_univs'] = len(univs_found)
    found_details['percent_univs'] = (len(univs_found)*100.00)/count_total_univs
    
    
    all_details = {}
    all_details['count_univs'] = count_total_univs
    
    all_countries_univs_found_not_found[country_name]['not_found'] = not_found_details
    all_countries_univs_found_not_found[country_name]['found'] = found_details
    all_countries_univs_found_not_found[country_name]['all'] = all_details
    
    
        
    print("Computed seniority distribution of authors in all univs in "+country_name+"\n")


# In[13]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_seniority_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_seniority_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[ ]:





# In[ ]:





# # Load data from previously saved files

# In[14]:


with open(join(output_dir,'all_countries_all_univs_seniority_info.txt')) as file:
     all_countries_all_univs_seniority_info = json.load(file)
        
print(all_countries_all_univs_seniority_info)


# # Create bar plot for each of the countries

# In[15]:


def create_sorted_plot(input_df, sorting_field_count, ylabel, xlabel, save_fname, save_file):
    
    # sort the df based on the sorting_field_percent
    df = input_df.sort_values(sorting_field_count, ascending=False)[['univs_name', sorting_field_count]]


    # Setting the positions and width for the bars
    pos = list(range(len(df['univs_name']))) 
    width = 0.25 

    # Plotting the bars
    fig, ax = plt.subplots(figsize=(25,10))

    # Create a bar with sorting_field_count data,
    # in position pos,
    sorting_field_bar = ax.bar(pos, 
            #using df['proportion_univs_agriculture'] data,
            df[sorting_field_count], 
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color= 'blue', 
            )
    
    # Set the y axis label
    ax.set_ylabel(ylabel)

    # Set the x axis label
    ax.set_xlabel(xlabel)

    # Set the position of the x ticks
    ax.set_xticks([p + 0.5 * width for p in pos])

    # Set the labels for the x ticks
    ax.set_xticklabels(df['univs_name'], rotation='vertical')

    # Setting the x-axis and y-axis limits
    plt.xlim(min(pos)-width, max(pos)+width*4)
    plt.ylim([0, max(df[sorting_field_count])])

    # Adding the legend and showing the plot
#     plt.legend(legend_text, loc='upper left')
    plt.grid()
    
    
    if save_file:
        plt.savefig(save_fname+".png", bbox_inches='tight', dpi=300)
        plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    return fig

def create_seniority_count_distribution_bar_chart(univs_details, save_fname, x_label, save_file=True):
    # https://chrisalbon.com/python/data_visualization/matplotlib_grouped_bar_plot/
    # https://stackoverflow.com/a/42498711/530399
    
    univs_name = [x for x in univs_details.keys()]
    univs_data = univs_details.values()
    univs_seniority_count = [x['avg_seniority_years'] for x in univs_data]
    
    
    raw_data = {'univs_name': univs_name,
        'univs_seniority_count': univs_seniority_count
               }
    df = pd.DataFrame(raw_data, columns = ['univs_name', 'univs_seniority_count'])

    
    xlabel = x_label + " -- Ranked by "
    ylabel = "Average Seniority Years"
    
    
    sorted_plot1 = create_sorted_plot(df, 'univs_seniority_count', ylabel, xlabel + "Average Seniority Years of Authors", save_fname = save_fname, save_file=save_file)
    
    return sorted_plot1


# In[16]:


country_name = 'austria'
univs_details = all_countries_all_univs_seniority_info[country_name]


sorted_plot1 = create_seniority_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'seniority_years_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=False)


# In[17]:


sorted_plot1


# In[18]:


for country_name, univs_details in all_countries_all_univs_seniority_info.items():
    create_seniority_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'seniority_years_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=True)


# In[ ]:





# In[19]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:




