#!/usr/bin/env python
# coding: utf-8

# # This will create plots for institutions of universities in THE WUR univs only and for the period of 2007-2017. The input dataset contains info of THE WUR univs only but for any period of time.

# #### The unpaywall dump used was from (April or June) 2018; hence analysis until 2017 only is going to be included.

# ## Question : What is the distribution of references (outgoing) for open access articles vs subscription based articles in papers published by the university?

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

import unicodedata
import re
from statistics import mean 


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


output_dir = join(root,"documents/analysis/dataset_selection_question6")


# In[8]:


# Create a new directory to save results
os.makedirs(output_dir)


# In[9]:


study_years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Extraction of OA and unknown status counts for papers referenced by publications coming from each university.

# In[10]:


def get_univ_papers_references_counts(country_papers_OA_df, univs_name):
    '''
    Get the plot of count OA and non-OA papers referenced by all publications from each university in the input country
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
        univ_papers_df_set1 = country_papers_OA_df[country_papers_OA_df['normalizedname']==THE_univ_name_normalised]
        
        univ_papers_df_set2 = country_papers_OA_df[country_papers_OA_df['normalizedwikiname']==THE_univ_name_normalised]
        
        # The records in two sets can be the excatly the same 
        # Concat and remove exact duplicates  -- https://stackoverflow.com/a/21317570/530399
        univ_papers_df = pd.concat([univ_papers_df_set1, univ_papers_df_set2]).drop_duplicates().reset_index(drop=True)
        

#         Put additional criteria that these papers are from 2007 till 2017
        univ_papers_df = univ_papers_df[univ_papers_df['year'].isin(study_years)]
        
        
        # Same paper will have multiple entries if there are multiple authors for that paper from same university.
        # This is not necessary because the input dataset was already prepared to exclude such duplicates.
#         univ_papers_df = univ_papers_df.drop_duplicates(subset="paperid")


        
        count_total_univ_papers = len(univ_papers_df)
        
        
        # For those I couldn't match/find their name, it is not fair to say that their OA count is 0. Should be excluded from the graph.
        if count_total_univ_papers==0:
            univs_not_found.append(org_univ_name+"    @    "+THE_univ_name_normalised)
        else:
            univs_found.append(org_univ_name)
            
            univs_info[org_univ_name] = {}
            
#           int casting needed to convert numpy int (json-incompatible) to python int
            count_OA_univ_referenced_papers = int(univ_papers_df['count_OA_references'].sum())
            count_unknown_univ_referenced_papers = int(univ_papers_df['count_unknown_references'].sum())
            
            count_total_univ_referenced_papers = count_OA_univ_referenced_papers + count_unknown_univ_referenced_papers

            univ_oa_references_percent = (count_OA_univ_referenced_papers*100.00)/count_total_univ_referenced_papers
            univ_other_references_percent = (count_unknown_univ_referenced_papers*100.00)/count_total_univ_referenced_papers
            
        
            
            
            univs_info[org_univ_name]["count_OA_referenced_papers"] = count_OA_univ_referenced_papers
            univs_info[org_univ_name]["percent_OA_referenced_papers"] = univ_oa_references_percent
            
            univs_info[org_univ_name]["count_unknown_referenced_papers"] = count_unknown_univ_referenced_papers
            univs_info[org_univ_name]["percent_unknown_referenced_papers"] = univ_other_references_percent
            
            univs_info[org_univ_name]["count_total_referenced_papers"] = count_total_univ_referenced_papers
        
    return univs_info, univs_not_found, univs_found


# In[11]:


all_countries_all_univs_OA_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    print("\nProcesing for dataset of univs in "+country_name+"\n")
    all_countries_univs_found_not_found[country_name] =  {}
    
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_OA_df = pd.read_csv(join(root,"data/processed/rc_oa_"+country_name+"_papers.csv"), header=0, sep=",", dtype={"year": object, "wikipage": object, "normalizedwikiname": object, 'count_OA_references': object,  "count_unknown_references": object})  # object means string
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_OA_df.drop(country_papers_OA_df[country_papers_OA_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_OA_df = country_papers_OA_df.astype({'year':int})
    country_papers_OA_df = country_papers_OA_df.astype({'count_OA_references':int})
    country_papers_OA_df = country_papers_OA_df.astype({'count_unknown_references':int})
    
    
    univs_info, univs_not_found, univs_found = get_univ_papers_references_counts(country_papers_OA_df, univs_name)
    
    all_countries_all_univs_OA_info[country_name] =  univs_info
    
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
    
    
        
    print("Computed references counts for all univs in "+country_name+"\n")


# In[12]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_OA_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_OA_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[ ]:





# # Load data from previously saved files

# In[13]:


with open(join(output_dir,'all_countries_all_univs_OA_info.txt')) as file:
     all_countries_all_univs_OA_info = json.load(file)
        
# all_countries_all_univs_OA_info


# # Create bar plot for each of the countries

# In[14]:


def label_bar_with_value(ax, rects, value_labels):
    """
    Attach a text label above each bar displaying its height
    """
    for i in range(len(rects)):
        rect = rects[i]
        label_value = value_labels[i]
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*rect.get_height(),
                '%s' % label_value,
                ha='center', va='bottom')

def create_reference_count_distribution_bar_chart(univs_details, save_fname, x_label, save_file=True):
    # https://chrisalbon.com/python/data_visualization/matplotlib_grouped_bar_plot/
    # https://stackoverflow.com/a/42498711/530399
    
    univs_name = [x for x in univs_details.keys()]
    univs_data = univs_details.values()
    univs_oa_reference_counts = [x['count_OA_referenced_papers'] for x in univs_data]
    univs_unknown_reference_counts = [x['count_unknown_referenced_papers'] for x in univs_data]
    
    
    raw_data = {'univs_name': univs_name,
        'univs_oa_reference_counts': univs_oa_reference_counts,
        'univs_unknown_reference_counts': univs_unknown_reference_counts
               }
    df = pd.DataFrame(raw_data, columns = ['univs_name', 'univs_oa_reference_counts', 'univs_unknown_reference_counts'])
    
    # Compute proportion of univs_oa_reference_counts
    df['proportion_univs_oa_reference_counts'] = (df['univs_oa_reference_counts'] / (df['univs_oa_reference_counts'] + df['univs_unknown_reference_counts'])) *100
    # sort the df based on proportion of univs_oa_reference_counts 
    df = df.sort_values('proportion_univs_oa_reference_counts', ascending=False)[['univs_name', 'univs_oa_reference_counts','univs_unknown_reference_counts', 'proportion_univs_oa_reference_counts']]
    
    # Setting the positions and width for the bars
    pos = list(range(len(df['univs_name']))) 
    width = 0.25 

    # Plotting the bars
    fig, ax = plt.subplots(figsize=(25,10))

    # Create a bar with oa_reference_count data,
    # in position pos,
    oa_reference_count_bars = ax.bar(pos, 
            #using df['univs_oa_reference_counts'] data,
            df['univs_oa_reference_counts'], 
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color='green', 
            )
    # Set heights based on the percentages
    oa_reference_counts_proportion_value_labels = [str(int(x))+"%" for x in df['proportion_univs_oa_reference_counts'].values.tolist()]

    # Create a bar with unknown_reference_count data,
    # in position pos + some width buffer,
    plt.bar([p + width for p in pos], 
            #using df['univs_unknown_reference_counts'] data,
            df['univs_unknown_reference_counts'],
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color='red', 
            ) 

    # Set the y axis label
    ax.set_ylabel('Outgoing Reference Counts')

    # Set the x axis label
    ax.set_xlabel(x_label)

    # Set the position of the x ticks
    ax.set_xticks([p + 0.5 * width for p in pos])

    # Set the labels for the x ticks
    ax.set_xticklabels(df['univs_name'], rotation='vertical')

    # Setting the x-axis and y-axis limits
    plt.xlim(min(pos)-width, max(pos)+width*4)
    plt.ylim([0, max(df['univs_oa_reference_counts'] + df['univs_unknown_reference_counts'])] )

    # Adding the legend and showing the plot
    plt.legend(['OA reference Counts', 'Unknown reference Counts'], loc='upper left')
    plt.grid()
    
    label_bar_with_value(ax, oa_reference_count_bars, oa_reference_counts_proportion_value_labels)
    
    if save_file:
        plt.savefig(save_fname+".png", bbox_inches='tight', dpi=300)
        plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    return fig


# In[15]:


country_name = 'austria'
univs_details = all_countries_all_univs_OA_info[country_name]

create_reference_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'referencescount_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=False)


# In[16]:


for country_name, univs_details in all_countries_all_univs_OA_info.items():
    create_reference_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'referencescount_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=True)


# In[ ]:





# In[17]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:





# In[ ]:




