#!/usr/bin/env python
# coding: utf-8

# # This will create plots for institutions of universities in THE WUR univs only and for the period of 2007-2017. The input dataset contains info of THE WUR univs only but for any period of time.

# #### This is to be compatible with other analysis questions which used dataset from the period of 2007 to 2017

# # Note: On-MERRIT DoW mentions Agriculture, Climate and Health but MAG has Agriculture, Climatology and Medicine.

# ## Question : How are papers published by the universities distributed across the three scientific disciplines of our choice?

# In[2]:


# standard path wrangling to be able to import project config and sources
import os
import sys
from os.path import join
root = os.path.dirname(os.getcwd())
sys.path.append(root)
print('Project root: {}'.format(root))


# In[3]:


sys.path.append(join(root,"spark/shared/"))
from MAG_utils import *


# In[ ]:





# In[4]:


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


# In[5]:


cfg = None
with open(join(root,"spark/config.json")) as fp:
    cfg = json.load(fp)


# In[6]:


# cfg


# In[7]:


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


# In[8]:


output_dir = join(root,"documents/analysis/dataset_selection_question2")


# In[ ]:


# Create a new directory to save results
os.makedirs(output_dir)


# In[9]:


study_years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# Load the external file -- fos_hierarchy to use for selecting papers that belong to our field of study choices.


# In[ ]:


generic = lambda x: ast.literal_eval(x)
conv = {"fieldofstudyid": int, "normalizedname": str, "level": int, "child_ids": str}  


fos_hierarchy = pd.read_csv(join(root,"data/external/fos_hierarchy.csv"), header=0, converters=conv, sep=",")  # object means string


# In[ ]:


fos_hierarchy.head(17).tail(8)


# In[ ]:


fos_hierarchy['child_ids_list'] = [[int(idx) for idx in x.split(",") if x!=''] for x in fos_hierarchy['child_ids']]

fos_hierarchy = fos_hierarchy.drop('child_ids', 1)


# In[ ]:


fos_hierarchy.head(17).tail(8)


# ### Identify the path to all the topmost parent (i.e. whose fos_name is eithere agriculture or climatology or medicine) for any the fos id.

# In[ ]:


relevant_fos_names = ["medicine","climatology", "agriculture"]

relevant_fos_ids = fos_hierarchy[fos_hierarchy['normalizedname'].isin(relevant_fos_names)]['fieldofstudyid'].values.tolist()
# print(relevant_fos_ids)

relevant_fos_dict = {}
for i in relevant_fos_ids:
    relevant_fos_dict[i] = fos_hierarchy[fos_hierarchy['fieldofstudyid']==i]['normalizedname'].values.tolist()[0]
print(relevant_fos_dict)


# In[ ]:


def get_immediate_parent_ids(fos_id):
    '''
    Returns a list of immediate parents (fieldofstudyid) for a given fos_id.
    '''
    # https://stackoverflow.com/a/41518959/530399
    mask = fos_hierarchy.child_ids_list.apply(lambda x: fos_id in x)
    all_parent_fos_df = fos_hierarchy[mask]
    all_parent_ids = all_parent_fos_df.fieldofstudyid.values.tolist()
    return all_parent_ids


# In[ ]:


def get_fos_parents(fos_id):
    '''
    For a given fos_id, explores the hierarchy all the way to the top to find parents that belong to either agriculture, climatology or medicine. 
    '''
    ids_up_hierarchy = []
    found_names = set()
    
    if fos_id in relevant_fos_ids:
        found_names.add(relevant_fos_dict[fos_id])
        return ids_up_hierarchy, found_names
    else:
        immediate_parents = get_immediate_parent_ids(fos_id)
        
        if not immediate_parents: # if we have reached to the top parent but it is still not the fos of our choice
            return [], set()
        else:
            to_explore_ids = []
            for i in immediate_parents:
                if i in relevant_fos_ids:
                    found_names.add(relevant_fos_dict[i])
                else:
                    ids_up_hierarchy.append(i)
                    to_explore_ids.append(i)
            
#             print("immediate parent ids = "+str(immediate_parents) + " and to explore ids = "+str(to_explore_ids))
            
            for j in to_explore_ids:
                x, y = get_fos_parents(j)
                ids_up_hierarchy.extend(x)
                found_names = (found_names | y)  # | means union
            
        return ids_up_hierarchy, found_names






# eg_fosid = 49204034
# eg_fosid = 2909824727
eg_fosid = 2781369281
# eg_fosid = 2778072252
# eg_fosid = 40520153
# eg_fosid = 2

parent_ids, parent_names = get_fos_parents(eg_fosid)
print(parent_ids, parent_names)


# In[ ]:


all_fosids = []
all_fosids.extend(y for x in fos_hierarchy['child_ids_list'].tolist() for y in x)
all_fosids.extend(x for x in fos_hierarchy['fieldofstudyid'].tolist())
all_fosids = list(set(all_fosids))
# len(all_fosids)

all_fosids_parentnames= {}
for x in all_fosids:
    _, parent_names = get_fos_parents(x)
    all_fosids_parentnames[x] = parent_names


# In[ ]:


def map_disciplines(input_fos_id):
    is_medicine = 0
    is_climatology = 0
    is_agriculture = 0
    if input_fos_id in all_fosids_parentnames:
        parent_names = all_fosids_parentnames[input_fos_id]
        if "medicine" in parent_names:
            is_medicine = 1
        if "climatology" in parent_names:
            is_climatology = 1
        if "agriculture" in parent_names:
            is_agriculture = 1
    return str(is_medicine)+","+str(is_climatology)+","+str(is_agriculture)


print(map_disciplines(eg_fosid))


# In[ ]:





# In[ ]:





# In[ ]:





# # Extraction of count of papers in each of the three disciplines for  publications coming from each university.

# In[ ]:


def get_univ_papers_disciplines_counts(country_papers_fos_df, univs_name):    
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
            
            # because there can be multiple records with the same paperid for the same university
            count_total_univ_papers = len(univ_papers_df.drop_duplicates(subset=['paperid']))
            
            univs_info[org_univ_name] = {}
            
            
            
            
            # Experimental
#             univ_papers_df = univ_papers_df.head(10)
            
            # Get the fieldofstudy for each paper in the univ_papers_df dataframe. This dataframe can contain multiple records for the same paperid because a paper could belong to multiple fields of study -- https://stackoverflow.com/a/27385043/530399
#         univ_papers_df['is_medicine'], univ_papers_df['is_climatology'], univ_papers_df['is_agriculture'] = zip(*univ_papers_df['fieldofstudyid'].map(map_disciplines))
            univ_papers_df['paper_fos_names_flag'] = univ_papers_df['fieldofstudyid'].swifter.apply(map_disciplines)
            univ_papers_df['is_medicine'], univ_papers_df['is_climatology'], univ_papers_df['is_agriculture'] = univ_papers_df['paper_fos_names_flag'].str.split(',').str
            univ_papers_df = univ_papers_df.astype({'is_medicine':int,'is_climatology':int,'is_agriculture':int})
            
            
#           int casting needed to convert numpy int (json-incompatible) to python int
            count_medicine_univ_papers = int(univ_papers_df['is_medicine'].sum())
            count_climatology_univ_papers = int(univ_papers_df['is_climatology'].sum())
            count_agriculture_univ_papers = int(univ_papers_df['is_agriculture'].sum())
            
            

            univ_medicine_percent = (count_medicine_univ_papers*100.00)/count_total_univ_papers
            univ_climatology_percent = (count_climatology_univ_papers*100.00)/count_total_univ_papers
            univ_agriculture_percent = (count_agriculture_univ_papers*100.00)/count_total_univ_papers
            
        
            
            
            univs_info[org_univ_name]["count_medicine_papers"] = count_medicine_univ_papers
            univs_info[org_univ_name]["percent_medicine_papers"] = univ_medicine_percent
            
            univs_info[org_univ_name]["count_climatology_papers"] = count_climatology_univ_papers
            univs_info[org_univ_name]["percent_climatology_papers"] = univ_climatology_percent
            
            univs_info[org_univ_name]["count_agriculture_papers"] = count_agriculture_univ_papers
            univs_info[org_univ_name]["percent_agriculture_papers"] = univ_agriculture_percent
            
            univs_info[org_univ_name]["count_total_papers"] = count_total_univ_papers
        
    return univs_info, univs_not_found, univs_found


# In[ ]:


all_countries_all_univs_fos_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    print("\nProcesing for dataset of univs in "+country_name+"\n")
    all_countries_univs_found_not_found[country_name] =  {}
    
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_fos_df = pd.read_csv(join(root,"data/processed/fsid_"+country_name+"_papers.csv"), header=0, sep=",", dtype={"year": object, "wikipage": object, "normalizedwikiname": object, 'fieldofstudyid': object,  "score": object})  # object means string
    
        
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_fos_df.drop(country_papers_fos_df[country_papers_fos_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_fos_df = country_papers_fos_df.astype({'year':int})
    country_papers_fos_df = country_papers_fos_df.astype({'fieldofstudyid':int})
    country_papers_fos_df = country_papers_fos_df.astype({'score':float})
    
    
    univs_info, univs_not_found, univs_found = get_univ_papers_disciplines_counts(country_papers_fos_df, univs_name)
    
    all_countries_all_univs_fos_info[country_name] =  univs_info
    
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
    
    
        
    print("Computed counts of papers in each disciplines for all univs in "+country_name+"\n")


# In[ ]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_fos_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_fos_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[ ]:





# In[ ]:





# # Load data from previously saved files

# In[10]:


with open(join(output_dir,'all_countries_all_univs_fos_info.txt')) as file:
     all_countries_all_univs_fos_info = json.load(file)
        
print(all_countries_all_univs_fos_info)


# # Create bar plot for each of the countries

# In[22]:


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


def create_fos_count_distribution_bar_chart(univs_details, save_fname, x_label, save_file=True):
    # https://chrisalbon.com/python/data_visualization/matplotlib_grouped_bar_plot/
    # https://stackoverflow.com/a/42498711/530399
    
    univs_name = [x for x in univs_details.keys()]
    univs_data = univs_details.values()
    univs_agriculture_counts = [x['count_agriculture_papers'] for x in univs_data]
    univs_climatology_counts = [x['count_climatology_papers'] for x in univs_data]
    univs_medicine_counts = [x['count_medicine_papers'] for x in univs_data]
    
    
    raw_data = {'univs_name': univs_name,
        'univs_agriculture_counts': univs_agriculture_counts,
        'univs_climatology_counts' : univs_climatology_counts,
        'univs_medicine_counts': univs_medicine_counts,
        'percent_agriculture_papers': percent_agriculture_papers,
        'percent_climatology_papers': percent_climatology_papers,
        'percent_medicine_papers': percent_medicine_papers
               }
    df = pd.DataFrame(raw_data, columns = ['univs_name', 'univs_agriculture_counts', 'univs_climatology_counts', 'univs_medicine_counts', 'percent_agriculture_papers', 'percent_climatology_papers', 'percent_medicine_papers'])
    
    # Compute proportion of each the fos counts for the university
    df['percent_agriculture_papers'] = df['percent_agriculture_papers'] *100
#     df['proportion_univs_climatology'] = df['percent_climatology_papers'] *100
#     df['proportion_univs_medicine'] = df['percent_medicine_papers'] *100
    
    
    print(df)
    
    # sort the df based on proportion of agriculture fos
    df = df.sort_values('proportion_univs_agriculture', ascending=False)[['univs_name', 'univs_agriculture_counts','univs_climatology_counts', 'univs_medicine_counts', 'proportion_univs_agriculture']]
#     df_sorted_climatology = df.sort_values('proportion_univs_climatology', ascending=False)[['univs_name', 'univs_agriculture_counts','univs_climatology_counts', 'univs_medicine_counts', 'proportion_univs_climatology']]
#     df_sorted_medicine = df.sort_values('proportion_univs_medicine', ascending=False)[['univs_name', 'univs_agriculture_counts','univs_climatology_counts', 'univs_medicine_counts', 'proportion_univs_medicine']]
    
    # Setting the positions and width for the bars
    pos = list(range(len(df['univs_name']))) 
    width = 0.25 

    # Plotting the bars
    fig, ax = plt.subplots(figsize=(25,10))

    # Create a bar with agriculture_count data,
    # in position pos,
    oa_reference_count_bars = ax.bar(pos, 
            #using df['proportion_univs_agriculture'] data,
            df['univs_agriculture_counts'], 
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color='green', 
            )
    # Set heights based on the percentages
    fos_agriculture_proportion_value_labels = [str(int(x))+"%" for x in df['proportion_univs_agriculture'].values.tolist()]
#     fos_agriculture_proportion_value_labels = [str(int(x))+"%" for x in df['proportion_univs_climatology'].values.tolist()]
#     fos_agriculture_proportion_value_labels = [str(int(x))+"%" for x in df['proportion_univs_medicine'].values.tolist()]

    # Create a bar with climatology_count data,
    # in position pos + some width buffer,
    plt.bar([p + width for p in pos], 
            #using df['univs_climatology_counts'] data,
            df['univs_climatology_counts'],
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color='red', 
            )
    
    # Create a bar with medicine_count data,
    # in position pos + 2*some width buffer,
    plt.bar([p + 2*width for p in pos], 
            #using df['univs_medicine_counts'] data,
            df['univs_medicine_counts'],
            # of width
            width, 
            # with alpha 0.5
            alpha=0.5, 
            # with color
            color='blue', 
            )
    
    
    

    # Set the y axis label
    ax.set_ylabel('Paper Counts')

    # Set the x axis label
    ax.set_xlabel(x_label)

    # Set the position of the x ticks
    ax.set_xticks([p + 0.5 * width for p in pos])

    # Set the labels for the x ticks
    ax.set_xticklabels(df['univs_name'], rotation='vertical')

    # Setting the x-axis and y-axis limits
    plt.xlim(min(pos)-width, max(pos)+width*4)
    plt.ylim([0, max(df['univs_agriculture_counts'] + df['univs_climatology_counts'] + df['univs_medicine_counts'])] )

    # Adding the legend and showing the plot
    plt.legend(['Agriculture paper Counts', 'Climatology Paper Counts', 'Medicine Paper Counts'], loc='upper left')
    plt.grid()
    
    label_bar_with_value(ax, oa_reference_count_bars, oa_reference_counts_proportion_value_labels)
    
    if save_file:
        plt.savefig(save_fname+".png", bbox_inches='tight', dpi=300)
        plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    return fig


# In[23]:


country_name = 'austria'
univs_details = all_countries_all_univs_fos_info[country_name]


create_fos_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'foscount_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]+" -- Ranked by papers in the "+"agriculture"+" discipline"), save_file=False)


# In[ ]:


'''for country_name, univs_details in all_countries_all_univs_fos_info.items():
    create_fos_count_distribution_bar_chart(univs_details, save_fname = join(output_dir,country_name+"_"+'foscount_distribution'), x_label = ("Universities in "+cnames_for_plot[country_name]), save_file=True)'''


# In[ ]:





# In[ ]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:





# In[ ]:




