#!/usr/bin/env python
# coding: utf-8

# # This will create plots for institutions of type universities only and for the period of 2007-2017. The input dataset contains info on universities as well as other institutions and for any period of time. The universities list comes from Times Higher Education (THE WUR).

# #### The unpaywall dump used was from (April or June) 2018; hence analysis until 2017 only is going to be included.

# #### The unpaywall dump used was from (April or June) 2018; hence analysis until 2017 only is going to be included.

# ## Question : What % of papers published by our selected universities in selected countries are Open Access?

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
plt.rcParams['pdf.fonttype'] = 42  # https://tex.stackexchange.com/a/508961/3741
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


output_dir = join(root,"documents/analysis/jcdl_dataset_question")
# Create a new directory to save results
os.makedirs(output_dir)


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


# In[21]:


def create_OA_percent_bar_chart(oa_percent_dict, save_fname, x_label=None, plt_text=None, display_values=False, sort_by_keys=True, figuresize=(15,10), ylimit=[0,100]):
    #     https://stackoverflow.com/a/37266356/530399
    if sort_by_keys:
        sorted_dict = sorted(oa_percent_dict.items(), key=lambda kv: kv[0]) # sorted by keys, return a list of tuples
    else:
        sorted_dict = sorted(oa_percent_dict.items(), key=lambda kv: kv[1]) # sorted by values
    x, y = zip(*sorted_dict) # unpack a list of pairs into two tuples
    
    
    plt.figure(figsize=figuresize)
    
    plt.bar(x,y)
    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label, fontsize=20)
    ax.set_ylabel("Percentage of OA papers published", fontsize=20)
    
    ax.xaxis.set_tick_params(labelsize=20)
    ax.yaxis.set_tick_params(labelsize=20)
    
    ax.set_ylim(ylimit)
    
    if plt_text:
#     https://stackoverflow.com/a/8482667/530399
        plt.text(0.7, 0.9,plt_text, ha='center', va='center', transform=ax.transAxes)
    
    if display_values:
        for i, v in enumerate(y):
            ax.text(i-.15, v + 2, str(round(v,2)), rotation=90, color='blue', fontweight='bold')
    
    plt.xticks(x, rotation='vertical')
    
    plt.savefig(save_fname+".png", bbox_inches='tight', dpi=600)
    plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=600)
    
    plt.close()
    
    return ax.get_figure()


# In[9]:


study_years = [2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017]


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Part A : Granularity Level of University Per Country

# In[10]:


def get_plt_univ_papers_OA_stats(country_papers_OA_df, univs_name):
    univs_oa_percent = {} # needed for plot data
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
            univs_info[org_univ_name]["count_total_papers"] = count_total_univ_papers
            
            # All (OA + unknown) 
            count_all_2007 = len(univ_papers_df[univ_papers_df['year']==2007])
            count_all_2008 = len(univ_papers_df[univ_papers_df['year']==2008])
            count_all_2009 = len(univ_papers_df[univ_papers_df['year']==2009])
            count_all_2010 = len(univ_papers_df[univ_papers_df['year']==2010])
            count_all_2011 = len(univ_papers_df[univ_papers_df['year']==2011])
            count_all_2012 = len(univ_papers_df[univ_papers_df['year']==2012])
            count_all_2013 = len(univ_papers_df[univ_papers_df['year']==2013])
            count_all_2014 = len(univ_papers_df[univ_papers_df['year']==2014])
            count_all_2015 = len(univ_papers_df[univ_papers_df['year']==2015])
            count_all_2016 = len(univ_papers_df[univ_papers_df['year']==2016])
            count_all_2017 = len(univ_papers_df[univ_papers_df['year']==2017])
            
            
            univs_info[org_univ_name]["yearwise_all"] = {}
            univs_info[org_univ_name]["yearwise_all"]["count_year"] = {
                "2007":count_all_2007,
                "2008":count_all_2008, "2009":count_all_2009, "2010":count_all_2010,"2011":count_all_2011,
                "2012":count_all_2012,"2013":count_all_2013,
                "2014":count_all_2014,"2015":count_all_2015,
                "2016":count_all_2016,"2017":count_all_2017
            }
            
            
            
            # OA part
            OA_univ_papers_df = univ_papers_df[univ_papers_df['is_OA']=="true"] # stored as a string in csv
            unknown_univ_papers_df = univ_papers_df[univ_papers_df['is_OA']!="true"] # stored as a string in csv
            
            count_OA_univ_papers = len(OA_univ_papers_df)
            count_unknown_univ_papers = len(unknown_univ_papers_df)

            univ_oa_percent = (count_OA_univ_papers*100.00)/count_total_univ_papers
            univ_other_percent = (count_unknown_univ_papers*100.00)/count_total_univ_papers
            
            univs_oa_percent[org_univ_name] = univ_oa_percent
        
            
            
            univs_info[org_univ_name]["count_OA_papers"] = count_OA_univ_papers
            univs_info[org_univ_name]["percent_OA_papers"] = univ_oa_percent
            
            univs_info[org_univ_name]["count_unknown_papers"] = count_unknown_univ_papers
            univs_info[org_univ_name]["percent_unknown_papers"] = univ_other_percent
            
            univs_info[org_univ_name]["count_total_papers"] = count_total_univ_papers
            
            
            
            # Further to get a yearwise breakdown of oa papers
            univs_info[org_univ_name]["yearwise_OA"] = {}            
            
            count_oa_2007 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2007])
            count_oa_2008 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2008])
            count_oa_2009 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2009])
            count_oa_2010 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2010])
            count_oa_2011 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2011])
            count_oa_2012 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2012])
            count_oa_2013 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2013])
            count_oa_2014 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2014])
            count_oa_2015 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2015])
            count_oa_2016 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2016])
            count_oa_2017 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2017])
            
            
            
            
            
            univs_info[org_univ_name]["yearwise_OA"]["count_year"] = {"2007":count_oa_2007, "2008":count_oa_2008,
                                                                  "2009":count_oa_2009, "2010":count_oa_2010,
                                                                  "2011":count_oa_2011, "2012":count_oa_2012,
                                                                  "2013":count_oa_2013, "2014":count_oa_2014,
                                                                  "2015":count_oa_2015, "2016":count_oa_2016,
                                                                  "2017":count_oa_2017}
    
    bar_fig = create_OA_percent_bar_chart(univs_oa_percent, save_fname = join(output_dir,country_name+"_"+'OA_percent') , x_label = ("Universities in "+country_name), plt_text = ('Total Count of Universities = '+str(len(univs_oa_percent))) )
    return bar_fig, univs_info, univs_not_found, univs_found


# In[ ]:


all_countries_plot = {}
all_countries_all_univs_OA_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    print("\nProcesing for dataset of univs in "+country_name+"\n")
    all_countries_plot[country_name] = {}
    all_countries_univs_found_not_found[country_name] =  {}
    
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_OA_df = pd.read_csv(join(root,"data/processed/OA_status_"+country_name+"_papers.csv"), header=0, sep=",", dtype={'is_OA': object, "url_lists_as_string": object, "year": object, "wikipage": object})  # object means string
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_OA_df.drop(country_papers_OA_df[country_papers_OA_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_OA_df = country_papers_OA_df.astype({'year':int})  # todo : for other types too including is_OA and update the check method to boolean type
    
    # Finally, create a new column named normalizedwikiname. This is helpful for matching english names of non-english universities. Eg: get "federal university of health sciences of porto alegre" for "universidade federal de ciencias da saude de porto alegre" using the wikilink which contains "universidade federal de ciencias da saude de porto alegre" in it.
    country_papers_OA_df["normalizedwikiname"] = country_papers_OA_df['wikipage'].apply(mag_normalisation_wiki_link)
    
    
    country_plot, univs_info, univs_not_found, univs_found = get_plt_univ_papers_OA_stats(country_papers_OA_df, univs_name)
    
    all_countries_plot[country_name] =  country_plot
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
    
    
        
    print("Saved plot for dataset of "+country_name+"\n")


# In[ ]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_OA_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_OA_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# # Load data from previously saved files

# In[10]:


with open(join(output_dir,'all_countries_all_univs_OA_info.txt')) as file:
     all_countries_all_univs_OA_info = json.load(file)
        
# all_countries_all_univs_OA_info


# # Create Representative universities OA percent comparision Scatter plot

# In[12]:


def create_representative_univs_line_plot_groups(all_countries_all_univs_OA_info, save_fname, x_label=None, y_label = "Percentage of OA Papers Published", plt_text=None):

    country_rep_univs = {}
    
    width = 0.9
    
    colors = ("red", "blue", "green")
    groups = ("Low Research Intensive Universities", "Medium Research Intensive Universities", "High Research Intensive Universities")
    
    
    high_tier_plot_data = []
    mid_tier_plot_data = []
    low_tier_plot_data = []
    
    
    country_tier_mean_values = []
    
    
    
    for country, univ_tiers in cfg["data"]["research_intensive_THE_WUR_institutions_by_country"].items():
#         print(country)
        country_rep_univs[cnames_for_plot[country]] = {}
    
        country_rep_univs[cnames_for_plot[country]]["High_Tier"]={}
        country_rep_univs[cnames_for_plot[country]]["Mid_Tier"]={}
        country_rep_univs[cnames_for_plot[country]]["Low_Tier"]={}
        

        high_tier_univs = univ_tiers["high"]
        for x in high_tier_univs:
            high_tier_plot_data.append((country+"(High)",all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]))
            country_rep_univs[cnames_for_plot[country]]["High_Tier"][x] = all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]
            
    
        medium_tier_univs = univ_tiers["medium"]
        for x in medium_tier_univs:
            mid_tier_plot_data.append((country+"(Mid)",all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]))
            country_rep_univs[cnames_for_plot[country]]["Mid_Tier"][x] = all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]
            
        
        low_tier_univs = univ_tiers["low"]
        for x in low_tier_univs:
            low_tier_plot_data.append((country+"(Low)",all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]))
            country_rep_univs[cnames_for_plot[country]]["Low_Tier"][x] = all_countries_all_univs_OA_info[country][x]["percent_OA_papers"]
    
    
    fig, axs = plt.subplots(1,1,figsize=(15,10), sharex=True, sharey=True)
    
    sorted_cnames = sorted(cfg["data"]["research_intensive_THE_WUR_institutions_by_country"].keys())
    
    hidden_tick_indices = []
    count_hidden_tick_index = -1
    for i in range(len(sorted_cnames)):
        cname = sorted_cnames[i]
        
        # First plot the data for low tier univs of the country
        country_low_tier_univs_values = [x[1] for x in low_tier_plot_data if x[0]==cname+"(Low)"]
#         axs.plot([cname+"(Low)"]*len(country_low_tier_univs_values), country_low_tier_univs_values, c="red", label="Low Tier University", linestyle='-', marker='o', linewidth=4)

        
        country_low_tier_mean_value = mean(country_low_tier_univs_values)
        country_low_tier_min_value = min(country_low_tier_univs_values)
        country_low_tier_max_value = max(country_low_tier_univs_values)
        country_tier_mean_values.append((cname+"(Low)",country_low_tier_mean_value))
        country_rep_univs[cnames_for_plot[cname]]["Low_Tier"]["Mean"] = country_low_tier_mean_value
        
        
        axs.scatter([cname+"(Low)"]*len(country_low_tier_univs_values), country_low_tier_univs_values, c="black", marker='x', label="OA %")
        height = country_low_tier_max_value - country_low_tier_min_value
        axs.add_patch(Rectangle(xy=(count_hidden_tick_index+1-width/2,country_low_tier_min_value-1) ,width=width, height=height+2, linewidth=1, color='cornflowerblue', fill="cornflowerblue", alpha=0.25, label="Low Tier Universities"))
        

        
        
        

        # Then plot the data for mid tier univs of the country    
        country_mid_tier_univs_values = [x[1] for x in mid_tier_plot_data if x[0]==cname+"(Mid)"]
#         axs.plot([cnames_for_plot[cname]]*len(country_mid_tier_univs_values), country_mid_tier_univs_values, c="orange", label="Mid Tier University", linestyle='-', marker='o', linewidth=4)  # to make this tick mark visible as cname rather than the true cname_mid; also capitalize the first letter

        
    
        country_mid_tier_mean_value = mean(country_mid_tier_univs_values)
        country_mid_tier_min_value = min(country_mid_tier_univs_values)
        country_mid_tier_max_value = max(country_mid_tier_univs_values)
        country_tier_mean_values.append((cnames_for_plot[cname],country_mid_tier_mean_value))
        country_rep_univs[cnames_for_plot[cname]]["Mid_Tier"]["Mean"] = country_mid_tier_mean_value
        
        
        axs.scatter([cnames_for_plot[cname]]*len(country_mid_tier_univs_values), country_mid_tier_univs_values, c="black", marker='x', label="OA %")
        height = country_mid_tier_max_value - country_mid_tier_min_value
        axs.add_patch(Rectangle(xy=(count_hidden_tick_index+2-width/2,country_mid_tier_min_value-1) ,width=width, height=height+2, linewidth=1, color='orange', fill="orange", alpha=0.25, label="Mid Tier Universities"))
        
        
        
        
        
    
        # Also, plot the data for high tier univs of the country
        country_high_tier_univs_values = [x[1] for x in high_tier_plot_data if x[0]==cname+"(High)"]
#         axs.plot([cname+"(High)"]*len(country_high_tier_univs_values), country_high_tier_univs_values, c="green", label="High Tier University", linestyle='-', marker='o', linewidth=4)
        
        
        country_high_tier_mean_value = mean(country_high_tier_univs_values)
        country_high_tier_min_value = min(country_high_tier_univs_values)
        country_high_tier_max_value = max(country_high_tier_univs_values)
        country_tier_mean_values.append((cname+"(High)",country_high_tier_mean_value))
        country_rep_univs[cnames_for_plot[cname]]["High_Tier"]["Mean"] = country_high_tier_mean_value
        
        
        axs.scatter([cname+"(High)"]*len(country_high_tier_univs_values), country_high_tier_univs_values, c="black", marker='x', label="OA %")
        height = country_high_tier_max_value - country_high_tier_min_value
        axs.add_patch(Rectangle(xy=(count_hidden_tick_index+3-width/2,country_high_tier_min_value-1),width=width, height=height+2, linewidth=1, color='green', fill="green", alpha=0.25, label="High Tier Universities"))
        
        
        
        
        
        # Hide the tick marks for the low and high tier markers
        hidden_tick_indices.append(count_hidden_tick_index+1)  # low marker
        hidden_tick_indices.append(count_hidden_tick_index+3)  # high marker
        
        
        # Finally add three fake tick points for inter spacing among the groups
        if i!=(len(sorted_cnames)-1):  # except when the last true xticks have been added.
            count_hidden_tick_index = count_hidden_tick_index + 4
            axs.plot([cname+"(None1)"], 10.0, c="white", linestyle='-', marker='o')
            hidden_tick_indices.append(count_hidden_tick_index)

            count_hidden_tick_index = count_hidden_tick_index + 1
            axs.plot([cname+"(None2)"], 10.0, c="white", linestyle='-', marker='o')
            hidden_tick_indices.append(count_hidden_tick_index)

            count_hidden_tick_index = count_hidden_tick_index + 1
            axs.plot([cname+"(None3)"], 10.0, c="white", linestyle='-', marker='o')
            hidden_tick_indices.append(count_hidden_tick_index)
        
        
    
#     https://stackoverflow.com/a/13583251/530399
    xticks = axs.xaxis.get_major_ticks()
    for hidden_tick_index in hidden_tick_indices:
        xticks[hidden_tick_index].set_visible(False)
    
    
    # Plot the mean value line
#     axs.scatter(*zip(*country_tier_mean_values), label='Mean Value', s=280, facecolors='none', edgecolors='b')
    axs.scatter(*zip(*country_tier_mean_values), label='Mean OA %', c="red", marker='s', s=104)
    
    
    
    
    # show grid at every ticks
#     plt.grid()
# https://stackoverflow.com/a/39039520/530399
    axs.set_axisbelow(True)
    axs.yaxis.grid(color='lightgrey', linestyle='dashed')
    

    # Frequency of y-ticks
    # https://stackoverflow.com/a/12608937/530399
    stepsize=3
    start, end = axs.get_ylim()
    axs.yaxis.set_ticks(np.arange(1, end, stepsize))
    
    # Font size to use for ticks
    axs.xaxis.set_tick_params(labelsize=20)
    axs.yaxis.set_tick_params(labelsize=20)
    

    axs.set_ylabel(y_label, fontsize=24, labelpad=15)
    
    
    
    
    #     Remove multiple legends by unique entires. Because each country was separately adeed for each tiers, there are duplicate legend entries.
#     https://stackoverflow.com/a/13589144/530399
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys(), prop={'size': 16},
               loc='upper center', bbox_to_anchor=(0.5, 1.05),
          ncol=3, fancybox=True, shadow=True
              )  # location of legend -- https://stackoverflow.com/a/4701285/530399
    
    
    
    plt.savefig(save_fname+".png", bbox_inches='tight', dpi=900)
    plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    
    return fig, country_rep_univs


# In[13]:


rep_univ_OA_plot, country_rep_univs_data = create_representative_univs_line_plot_groups(all_countries_all_univs_OA_info, save_fname = join(output_dir,"all_countries_representative_univs_OA_percent"))

rep_univ_OA_plot


# In[ ]:


# Write country_rep_univs to file
with open(join(output_dir,'representative_univs_in_all_countries.txt'), 'w') as file:
     file.write(json.dumps(country_rep_univs_data, sort_keys=True, indent=4, ensure_ascii=False))


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Part B: Analysis at Country Level

# #### This can't build up on the data from univ_level because of duplicate paper. If the same paper(paperid) has authors from multiple univs within the same country, only one instance of it can be considered. 
# 
# #### 1. Load country level dataset 2. Retain records from unis in THE_WUR list only. 3. Delete duplicate paperid records 4. records from study_years only 4. Yearwise Breakdown

# In[11]:


countries_oa_info = {}
countries_oa_percents = {}  # needed for plot.

for country_name,univs_name in cfg['data']['all_THE_WUR_institutions_by_country'].items():
    
    countries_oa_info[country_name] = {}
    
    
    
    # 1. Load Data
    # CSV has repeated header from multiple partitions of the merge on pyspark csv output. Hence need to treat as string.
    country_papers_OA_df = pd.read_csv(join(root,"data/processed/OA_status_"+country_name+"_papers.csv"), header=0, sep=",", dtype={'is_OA': object, "url_lists_as_string": object, "year": object, "wikipage": object})  # object means string
    # Then eliminate problematic lines
    #  temp fix until spark csv merge header issue is resolved -- the header line is present in each re-partition's output csv
    country_papers_OA_df.drop(country_papers_OA_df[country_papers_OA_df.paperid == "paperid"].index, inplace=True)
    # Then reset dtypes as needed.
    country_papers_OA_df = country_papers_OA_df.astype({'year':int})  # todo : for other types too including is_OA and update the check method to boolean type
    
    
    # Finally, create a new column named normalizedwikiname. This is helpful for matching english names of non-english universities. Eg: get "federal university of health sciences of porto alegre" for "universidade federal de ciencias da saude de porto alegre" using the wikilink which contains "universidade federal de ciencias da saude de porto alegre" in it.
    country_papers_OA_df["normalizedwikiname"] = country_papers_OA_df['wikipage'].apply(mag_normalisation_wiki_link)
    
    
    # 2. Retain records from THE_WUR only
    univs_names_normalized = [mag_normalisation_institution_names(x) for x in univs_name]
    country_THE_papers_OA_df_set1 = country_papers_OA_df[country_papers_OA_df['normalizedname'].isin(univs_names_normalized)]
    country_THE_papers_OA_df_set2 = country_papers_OA_df[country_papers_OA_df['normalizedwikiname'].isin(univs_names_normalized)]
    # The records in two sets can be the excatly the same 
# Concat and remove exact duplicates  -- https://stackoverflow.com/a/21317570/530399
    country_THE_papers_OA_df = pd.concat([country_THE_papers_OA_df_set1, country_THE_papers_OA_df_set2]).drop_duplicates().reset_index(drop=True)
    
    
    # 3. Remove Duplicates paperids -- same paper with authors from multiple universities within the country.
    country_THE_papers_OA_df = country_THE_papers_OA_df.drop_duplicates(subset="paperid")
    
    # 4. Put criteria that these papers are from 2007 till 2017
    country_THE_papers_OA_df = country_THE_papers_OA_df[country_THE_papers_OA_df['year'].isin(study_years)]
    
    
    
    OA_papers = country_THE_papers_OA_df[country_THE_papers_OA_df['is_OA']=="true"]
    unknown_papers = country_THE_papers_OA_df[country_THE_papers_OA_df['is_OA']!="true"]
    
    
    count_country_OA_papers = len(OA_papers)
    count_country_unknown_papers = len(unknown_papers)
    
    total_country_papers = count_country_OA_papers + count_country_unknown_papers
    percent_OA_country = (count_country_OA_papers * 100.00)/total_country_papers
    percent_unknown_country = (count_country_unknown_papers * 100.00)/total_country_papers
    
    
    countries_oa_percents[country_name] = percent_OA_country
    
    countries_oa_info[country_name]['count_OA_papers'] = count_country_OA_papers
    countries_oa_info[country_name]['count_unknown_papers'] = count_country_unknown_papers    
    countries_oa_info[country_name]['percent_OA_papers'] = percent_OA_country
    countries_oa_info[country_name]['percent_unknown_papers'] = percent_unknown_country
    countries_oa_info[country_name]['count_total_papers'] = total_country_papers
    
    
    
    # Yearwise Breakdown
    count_oa_2007 = len(OA_papers[OA_papers["year"]==2007])
    count_oa_2008 = len(OA_papers[OA_papers["year"]==2008])
    count_oa_2009 = len(OA_papers[OA_papers["year"]==2009])
    count_oa_2010 = len(OA_papers[OA_papers["year"]==2010])
    count_oa_2011 = len(OA_papers[OA_papers["year"]==2011])
    count_oa_2012 = len(OA_papers[OA_papers["year"]==2012])
    count_oa_2013 = len(OA_papers[OA_papers["year"]==2013])
    count_oa_2014 = len(OA_papers[OA_papers["year"]==2014])
    count_oa_2015 = len(OA_papers[OA_papers["year"]==2015])
    count_oa_2016 = len(OA_papers[OA_papers["year"]==2016])
    count_oa_2017 = len(OA_papers[OA_papers["year"]==2017])
   

    
    count_all_2007 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2007])
    count_all_2008 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2008])
    count_all_2009 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2009])
    count_all_2010 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2010])
    count_all_2011 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2011])
    count_all_2012 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2012])
    count_all_2013 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2013])
    count_all_2014 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2014])
    count_all_2015 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2015])
    count_all_2016 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2016])
    count_all_2017 = len(country_THE_papers_OA_df[country_THE_papers_OA_df["year"]==2017])
    
    
    
    
    
    
    
    
    
    
    countries_oa_info[country_name]["yearwise_OA"] = {}
    
    countries_oa_info[country_name]["yearwise_OA"]["count_year"] = {"2007":count_oa_2007, "2008":count_oa_2008,
                                                           "2009":count_oa_2009, "2010":count_oa_2010,
                                                           "2011":count_oa_2011, "2012":count_oa_2012,
                                                           "2013":count_oa_2013, "2014":count_oa_2014,
                                                           "2015":count_oa_2015, "2016":count_oa_2016,
                                                           "2017":count_oa_2017}

    
    # Lets find the percentage OA in each year
    countries_oa_info[country_name]["yearwise_OA"]["percent_year"] = {
        "2007":(count_oa_2007*100.00)/count_all_2007,
        "2008":(count_oa_2008*100.00)/count_all_2008,
       "2009":(count_oa_2009*100.00)/count_all_2009,
        "2010":(count_oa_2010*100.00)/count_all_2010,
       "2011":(count_oa_2011*100.00)/count_all_2011,
        "2012":(count_oa_2012*100.00)/count_all_2012,
       "2013":(count_oa_2013*100.00)/count_all_2013,
        "2014":(count_oa_2014*100.00)/count_all_2014,
       "2015":(count_oa_2015*100.00)/count_all_2015, 
        "2016":(count_oa_2016*100.00)/count_all_2016,
       "2017":(count_oa_2017*100.00)/count_all_2017
    }
    
    
    print("\nCompleted processing for dataset of "+country_name+"\n")
    


# In[12]:


with open(join(output_dir,'all_countries_OA_info.txt'), 'w') as file:
     file.write(json.dumps(countries_oa_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[13]:


countries_oa_percents


# In[24]:


countries_oa_percent_bar_plot = create_OA_percent_bar_chart({cnames_for_plot[key]:value for key, value in countries_oa_percents.items()}, save_fname = join(output_dir,"all_countries_OA_percent"), x_label = "Countries", display_values=True, sort_by_keys=False, figuresize=(8,8), ylimit=[0,40])


# In[25]:


countries_oa_percent_bar_plot


# In[ ]:





# In[23]:


def create_yearwise_OA_percent_line_chart(countries_oa_info, save_fname, x_label = "Year", plt_text=None):
    
    plt.figure(figsize=(15,10))
    
    country_names_list = []
    markers = ['o', 'x', 'v', 's', '*', '+', 'D', '|']
    for country_name,oa_info in countries_oa_info.items():
        
        percent_oa = oa_info["yearwise_OA"]["percent_year"]
        # sort by year
        #  https://stackoverflow.com/a/37266356/530399
        sort_by_year = sorted(percent_oa.items(), key=lambda kv: int(kv[0]))
        years, percent_oas = zip(*sort_by_year) # unpack a list of pairs into two tuples
        
        plt.plot(years, percent_oas, linewidth=4, markersize=12, marker=markers[len(country_names_list)])
        
        country_names_list.append(country_name)
        
        
    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label, fontsize=20, labelpad=10)
    ax.set_ylabel("% of OA paper published in each year", fontsize=24, labelpad=15)
    
    
    
    # Font size to use for ticks
    ax.xaxis.set_tick_params(labelsize=20)
    ax.yaxis.set_tick_params(labelsize=20)
    
    
    # Frequency of x-ticks
    # https://stackoverflow.com/a/12608937/530399
    stepsize=3
    start, end = ax.get_ylim()
    ax.yaxis.set_ticks(np.arange(int(start), end, stepsize))
    
    
    
    # show grid at every ticks
    #     plt.grid()
    # https://stackoverflow.com/a/39039520/530399
    ax.set_axisbelow(True)
    ax.yaxis.grid(color='lightgrey', linestyle='dashed')
    
    
    if plt_text:
#     https://stackoverflow.com/a/8482667/530399
        plt.text(0.7, 0.9,plt_text, ha='center', va='center', transform=ax.transAxes)
    
#     plt.xticks(years)
    plt.legend([cnames_for_plot[x] for x in country_names_list], loc='upper left', prop={'size': 16})
    
    plt.savefig(save_fname+".png", bbox_inches='tight', dpi=900)
    plt.savefig(save_fname+".pdf", bbox_inches='tight', dpi=900)
    
    plt.close()
    
    return ax.get_figure()


# In[24]:


countries_OA_percent_each_year_line_plot = create_yearwise_OA_percent_line_chart(countries_oa_info, save_fname = join(output_dir,"all_countries_OA_percent_each_year"), x_label = "Year")


# In[25]:


countries_OA_percent_each_year_line_plot


# In[26]:


countries_oa_info['usa']


# In[ ]:


# countries_oa_info['brazil']


# In[ ]:


# countries_oa_info['germany']


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:





# In[ ]:




