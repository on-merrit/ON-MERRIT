#!/usr/bin/env python
# coding: utf-8

# # This will create plots for institutions of universities in THE WUR univs only and for the period of 2007-2017. The input dataset contains info of THE WUR univs only but for any period of time.

# #### This is to be compatible with other analysis questions which used dataset from the period of 2007 to 2017

# #### Note: 
# 
# #### The gender in the csv are i)unknown (name not found), ii) andy (androgynous), iii) male, iv) female, v) mostly_male and vi) mostly_female following the schema used by the external library we used to determine the gender -- https://pypi.org/project/gender-guesser/ . The difference between andy and unknown is that the former is found to have the same probability to be male than to be female, while the later means that the name wasn’t found in the database.
# 
# #### For our purposes, i)unknow/andy --> unknown ii)male/mostly_male --> male and iii)female/mostly_female --> female

# ## Question : What is the gender distribution in authorship of papers published by the universities?

# In[4]:


# standard path wrangling to be able to import project config and sources
import os
import sys
from os.path import join
root = os.path.dirname(os.getcwd())
sys.path.append(root)
print('Project root: {}'.format(root))


# In[5]:


sys.path.append(join(root,"spark/shared/"))
from MAG_utils import *


# In[ ]:





# In[6]:


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


# In[7]:


cfg = None
with open(join(root,"spark/config.json")) as fp:
    cfg = json.load(fp)


# In[8]:


# cfg


# In[9]:


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


# In[10]:


output_dir = join(root,"documents/analysis/dataset_selection_question3")


# In[8]:


# Create a new directory to save results
os.makedirs(output_dir)


# In[11]:


study_years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]


# In[ ]:





# In[ ]:





# In[ ]:





# # Extraction of count of papers in each of the three gender categories for  publications coming from each university.

# In[10]:


def get_univ_authors_gender_counts(country_papers_fos_df, univs_name):    
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
            
            # here, we are going to count gender against the total records count -- because each paper could have multiple male (or female) authors. This dataset is about authorship, not about paper.
            count_total_authors = len(univ_papers_df)
            
            count_male_authors = len(univ_papers_df[univ_papers_df['gender']=="male"])+len(univ_papers_df[univ_papers_df['gender']=="mostly_male "])
            count_female_authors = len(univ_papers_df[univ_papers_df['gender']=="female"])+len(univ_papers_df[univ_papers_df['gender']=="mostly_female"])
            count_unknown_authors = len(univ_papers_df[univ_papers_df['gender']=="unknown"])+len(univ_papers_df[univ_papers_df['gender']=="andy"])
            
            
            univs_info[org_univ_name] = {}
            
            
            
            
            univ_male_percent = (count_male_authors*100.00)/count_total_authors
            univ_female_percent = (count_female_authors*100.00)/count_total_authors
            univ_unknown_percent = (count_unknown_authors*100.00)/count_total_authors
            
        
            
            
            univs_info[org_univ_name]["count_male_authors"] = count_male_authors
            univs_info[org_univ_name]["percent_male_authors"] = univ_male_percent
            
            univs_info[org_univ_name]["count_female_authors"] = count_female_authors
            univs_info[org_univ_name]["percent_female_authors"] = univ_female_percent
            
            univs_info[org_univ_name]["count_unknown_authors"] = count_unknown_authors
            univs_info[org_univ_name]["percent_unknown_authors"] = univ_unknown_percent
            
            univs_info[org_univ_name]["count_total_authors"] = count_total_authors
        
    return univs_info, univs_not_found, univs_found


# In[12]:


all_countries_all_univs_gender_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    print("\nProcesing for dataset of univs in "+country_name+"\n")
    all_countries_univs_found_not_found[country_name] =  {}
    
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_authors_df = pd.read_csv(join(root,"data/processed/author_gender_"+country_name+"_papers.csv"), header=0, sep=",", dtype={"year": object})  # object means string
    
        
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_authors_df.drop(country_papers_authors_df[country_papers_authors_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_authors_df = country_papers_authors_df.astype({'year':int})
    
    
    univs_info, univs_not_found, univs_found = get_univ_authors_gender_counts(country_papers_authors_df, univs_name)
    
    all_countries_all_univs_gender_info[country_name] =  univs_info
    
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
    
    
        
    print("Computed gender distribution of authors in all univs in "+country_name+"\n")


# In[13]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_gender_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_gender_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[ ]:





# In[ ]:





# # Load data from previously saved files

# In[12]:


with open(join(output_dir,'all_countries_all_univs_gender_info.txt')) as file:
     all_countries_all_univs_gender_info = json.load(file)
        
print(all_countries_all_univs_gender_info)


# # Create bar plot for each of the countries

# In[24]:


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

def create_sorted_plot(input_df, sorting_field_count, sorting_field_percent, other_fields_count, colors2plot, ylabel, xlabel, legend_text, save_fname, save_file):
    
    second_field_count = other_fields_count[0]
    third_field_count = other_fields_count[1]
    
    # sort the df based on the sorting_field_percent
    df = input_df.sort_values(sorting_field_percent, ascending=False)[['univs_name', sorting_field_count, second_field_count, third_field_count, sorting_field_percent]]


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
            color= colors2plot[sorting_field_count], 
            )
    # Create labels with percentage values
    sorting_field_proportion_value_labels = [str(int(x))+"%" for x in df[sorting_field_percent].values.tolist()]


    # Create a bar with second_field_count data,
    # in position pos + some width buffer,
    plt.bar([p + width for p in pos], 
            #using df['univs_climatology_counts'] data,
            df[second_field_count],
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color=colors2plot[second_field_count],
            )
    
    # Create a bar with third_field_count data,
    # in position pos + 2*some width buffer,
    plt.bar([p + 2*width for p in pos], 
            #using df['univs_medicine_counts'] data,
            df[third_field_count],
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color=colors2plot[third_field_count],
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
    plt.ylim([0, max(df[sorting_field_count] + df[second_field_count] + df[third_field_count])] )

    # Adding the legend and showing the plot
    plt.legend(legend_text, loc='upper left')
    plt.grid()
    
    label_bar_with_value(ax, sorting_field_bar, sorting_field_proportion_value_labels)
    
    if save_file:
        plt.savefig(save_fname+".png", bbox_inches='tight', dpi=300)
        plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    return fig

def create_gender_count_distribution_bar_chart(univs_details, save_fname, x_label, save_file=True):
    # https://chrisalbon.com/python/data_visualization/matplotlib_grouped_bar_plot/
    # https://stackoverflow.com/a/42498711/530399
    
    univs_name = [x for x in univs_details.keys()]
    univs_data = univs_details.values()
    univs_male_counts = [x['count_male_authors'] for x in univs_data]
    univs_female_counts = [x['count_female_authors'] for x in univs_data]
    univs_unknown_counts = [x['count_unknown_authors'] for x in univs_data]
    percent_male_authors = [x['percent_male_authors'] for x in univs_data]
    percent_female_authors = [x['percent_female_authors'] for x in univs_data]
    percent_unknown_authors = [x['percent_unknown_authors'] for x in univs_data]
    
    
    raw_data = {'univs_name': univs_name,
        'univs_male_counts': univs_male_counts,
        'univs_female_counts' : univs_female_counts,
        'univs_unknown_counts': univs_unknown_counts,
        'percent_male_authors': percent_male_authors,
        'percent_female_authors': percent_female_authors,
        'percent_unknown_authors': percent_unknown_authors
               }
    df = pd.DataFrame(raw_data, columns = ['univs_name', 'univs_male_counts', 'univs_female_counts', 'univs_unknown_counts', 'percent_male_authors', 'percent_female_authors', 'percent_unknown_authors'])    

    #     print(df)

    colors2plot={'univs_male_counts':'green', 'univs_female_counts':'red', 'univs_unknown_counts':'blue'}
    xlabel = x_label + " -- Ranked by "
    ylabel = "Authorship Counts"
    
    
    
    sorted_plot1 = create_sorted_plot(df, 'univs_male_counts', 'percent_male_authors', ['univs_female_counts', 'univs_unknown_counts'], colors2plot, ylabel, xlabel + "Male"+" Authorship Counts", legend_text=['Male Authorship Counts', 'Female Authorship Counts', 'Unknown Authorship Counts'], save_fname = save_fname+"_sorted_Male", save_file=save_file)
   
    sorted_plot2 = create_sorted_plot(df, 'univs_female_counts', 'percent_female_authors', ['univs_unknown_counts', 'univs_male_counts'], colors2plot, ylabel, xlabel + "Female"+" Authorship Counts", legend_text=['Female Authorship Counts', 'Unknown Authorship Counts', 'Male Authorship Counts'], save_fname = save_fname+"_sorted_Female", save_file=save_file)
    
    sorted_plot3 = create_sorted_plot(df, 'univs_unknown_counts', 'percent_unknown_authors', ['univs_male_counts', 'univs_female_counts'], colors2plot, ylabel, xlabel + "Unknown"+" Authorship Counts", legend_text=['Unknown Authorship Counts', 'Male Authorship Counts', 'Female Authorship Counts'], save_fname = save_fname+"_sorted_Unknown", save_file=save_file)
    
    return sorted_plot1, sorted_plot2, sorted_plot3


# In[25]:


country_name = 'austria'
univs_details = all_countries_all_univs_gender_info[country_name]


sorted_plot1, sorted_plot2, sorted_plot3 = create_gender_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'gender_count_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=False)


# In[26]:


sorted_plot1


# In[27]:


sorted_plot2


# In[28]:


sorted_plot3


# In[29]:


for country_name, univs_details in all_countries_all_univs_gender_info.items():
    create_gender_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'gender_count_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=True)


# In[ ]:





# In[30]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:




