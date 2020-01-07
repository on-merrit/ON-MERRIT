#!/usr/bin/env python
# coding: utf-8

# # This will create plots for institutions of type universities only. The universities list comes from Times Higher Education (THE).

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

import unicodedata
import re


# In[4]:


cfg = None
with open(join(root,"spark/config.json")) as fp:
    cfg = json.load(fp)


# In[5]:


# cfg


# In[6]:


output_dir = join(root,"documents/analysis/jcdl_dataset_question")


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[7]:


def create_OA_percent_bar_chart(oa_percent_dict, save_fname, x_label=None, plt_text=None, display_values=False):
    #     https://stackoverflow.com/a/37266356/530399
    sort_by_vals = sorted(oa_percent_dict.items(), key=lambda kv: kv[1], reverse=True) # sorted by values, return a list of tuples
    x, y = zip(*sort_by_vals) # unpack a list of pairs into two tuples
    
    
    plt.figure(figsize=(15,10))
    
    plt.bar(x,y)
    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label)
    ax.set_ylabel("Percentage of OA papers published")
    
    ax.set_ylim([0,100])
    
    if plt_text:
#     https://stackoverflow.com/a/8482667/530399
        plt.text(0.7, 0.9,plt_text, ha='center', va='center', transform=ax.transAxes)
    
    if display_values:
        for i, v in enumerate(y):
            ax.text(i-.25, v + 3, str(round(v,3)), color='blue', fontweight='bold')
    
    plt.xticks(x, rotation='vertical')
    
    plt.savefig(save_fname+".png", bbox_inches='tight')
    plt.savefig(save_fname+".pdf", bbox_inches='tight')
    
    plt.close()
    
    return ax.get_figure()


# In[ ]:





# In[ ]:





# In[ ]:





# In[8]:


def mag_wiki_link_normalise(wikilink):
#     Get the english name from the wiki

#     print(wikilink)
    
    try:
        last_slash_index = wikilink.rindex('/')
        start_index = last_slash_index+1
        uni_name = wikilink[start_index:]
    except:
        uni_name = ""
#     Apply MAG normalisation to the name
    return mag_normalisation_institution_names(uni_name)


# In[ ]:





# In[ ]:





# # Part A : Analysis Per University Per Country

# In[9]:


pub_year_bins = [0, 2010, 2013, 2016, 2019]


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
        
        
        # Concat and remove exact duplicates  -- https://stackoverflow.com/a/21317570/530399
        univ_papers_df = pd.concat([univ_papers_df_set1, univ_papers_df_set2]).drop_duplicates().reset_index(drop=True)
        
        
        
        
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
            count_all_2018 = len(univ_papers_df[univ_papers_df['year']==2018])
            
            
            univs_info[org_univ_name]["yearwise_all"] = {}
            univs_info[org_univ_name]["yearwise_all"]["count_year"] = {"2008":count_all_2008, "2009":count_all_2009, "2010":count_all_2010,"2011":count_all_2011,"2012":count_all_2012,"2013":count_all_2013,"2014":count_all_2014,"2015":count_all_2015,"2016":count_all_2016,"2017":count_all_2017,"2018":count_all_2018}
            
            
            
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
            count_oa_2018 = len(OA_univ_papers_df[OA_univ_papers_df['year']==2018])
            
            
            
            
            
            univs_info[org_univ_name]["yearwise_OA"]["count_year"] = {"2007":count_oa_2007, "2008":count_oa_2008,
                                                                  "2009":count_oa_2009, "2010":count_oa_2010,
                                                                  "2011":count_oa_2011, "2012":count_oa_2012,
                                                                  "2013":count_oa_2013, "2014":count_oa_2014,
                                                                  "2015":count_oa_2015, "2016":count_oa_2016,
                                                                  "2017":count_oa_2017, "2018":count_oa_2018} 
            
            
            bucket_year_groups = OA_univ_papers_df.groupby(pd.cut(OA_univ_papers_df.year, pub_year_bins))
            bucket_year_groups_count_records = bucket_year_groups.size().to_dict()
            #  for easy readbility
            remapped = {}
            remapped["0-2010"] = bucket_year_groups_count_records[pd.Interval(0, 2010, closed='right')]
            remapped["2011-2013"] = bucket_year_groups_count_records[pd.Interval(2010, 2013, closed='right')]
            remapped["2014-2016"] = bucket_year_groups_count_records[pd.Interval(2013, 2016, closed='right')]
            remapped["2017-2019"] = bucket_year_groups_count_records[pd.Interval(2016, 2019, closed='right')]
            
            univs_info[org_univ_name]["yearwise_OA"]["count_intervals"] = remapped
            
            
            
            
#             Note: This is the growth rate per year.
            growth_rate = {}  # change divided by the time it took to make that change
            growth_rate["2008"] = (count_oa_2008 - count_oa_2007)/1.00
            growth_rate["2009"] = (count_oa_2009 - count_oa_2008)/1.00
            growth_rate["2010"] = (count_oa_2010 - count_oa_2009)/1.00
            growth_rate["2011"] = (count_oa_2011 - count_oa_2010)/1.00
            growth_rate["2012"] = (count_oa_2012 - count_oa_2011)/1.00
            growth_rate["2013"] = (count_oa_2013 - count_oa_2012)/1.00
            growth_rate["2014"] = (count_oa_2014 - count_oa_2013)/1.00
            growth_rate["2015"] = (count_oa_2015 - count_oa_2014)/1.00
            growth_rate["2016"] = (count_oa_2016 - count_oa_2015)/1.00
            growth_rate["2017"] = (count_oa_2017 - count_oa_2016)/1.00
            growth_rate["2018"] = (count_oa_2018 - count_oa_2017)/1.00
            
            
            univs_info[org_univ_name]["yearwise_OA"]["growth_rate"] = growth_rate
    
    bar_fig = create_OA_percent_bar_chart(univs_oa_percent, save_fname = join(output_dir,country_name+"_"+'OA_percent') , x_label = ("Universities in "+country_name), plt_text = ('Total Count of Universities = '+str(len(univs_oa_percent))) )
    return bar_fig, univs_info, univs_not_found, univs_found


# In[11]:


# Create a new directory to save results
os.makedirs(output_dir)


all_countries_plot = {}
all_countries_all_univs_OA_info = {}
all_countries_univs_found_not_found = {}

for country_name,univs_name in cfg['data']['all_THE_institutions_by_country'].items():
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
    country_papers_OA_df["normalizedwikiname"] = country_papers_OA_df['wikipage'].apply(mag_wiki_link_normalise)
    
    
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





# In[12]:


# Write text files with the infos

with open(join(output_dir,'all_countries_univs_found_not_found.txt'), 'w') as file:
     file.write(json.dumps(all_countries_univs_found_not_found, sort_keys=True, indent=4, ensure_ascii=False))
        
with open(join(output_dir,'all_countries_all_univs_OA_info.txt'), 'w') as file:
     file.write(json.dumps(all_countries_all_univs_OA_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[ ]:





# In[ ]:





# In[ ]:





# # Sample Test country

# In[13]:


# test_country = "russia"
test_country = "brazil"
# test_country = "uk"
# test_country = "usa"


# In[14]:


# all_countries_plot[test_country].savefig(join(output_dir,test_country+"_"+'OA.pdf'), bbox_inches='tight')


# In[15]:


all_countries_plot[test_country]


# In[16]:


print(all_countries_univs_found_not_found[test_country]['not_found']['univ_names'])


# In[17]:


all_countries_univs_found_not_found[test_country]['not_found']['count_univs']


# In[ ]:





# # Sample University

# In[18]:


# test_univ = "Altai State Technical University"
test_univ = "Universidade Luterana do Brasil (ULBRA)"
# test_univ = "Brighton and Sussex Medical School"
# test_univ = "central baptist college"


# In[19]:


all_countries_all_univs_OA_info[test_country][test_univ]


# In[20]:


test_OA_value = 100.00
for key,val in all_countries_all_univs_OA_info[test_country].items():
    if val['percent_OA_papers'] == test_OA_value:
        print(key)
        print(val)
        print()


# In[ ]:





# In[21]:


max_OA_papers_count = 0
max_OA_papers_uni = None
for key,val in all_countries_all_univs_OA_info[test_country].items():
    if val['count_OA_papers'] > max_OA_papers_count:
        max_OA_papers_uni = key
        max_OA_papers_count = val['count_OA_papers']
print(max_OA_papers_uni)
print(all_countries_all_univs_OA_info[test_country][max_OA_papers_uni])


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# # Part B: Analysis at Country Level

# In[22]:


with open(join(output_dir,'all_countries_all_univs_OA_info.txt')) as file:
     all_countries_all_univs_OA_info = json.load(file)


# In[23]:


# all_countries_all_univs_OA_info


# In[24]:


countries_oa_info = {}
countries_oa_percents = {}  # needed for plot.

for key,val in all_countries_all_univs_OA_info.items():
#     print(key)
    count_country_OA_papers = 0
    count_country_unknown_papers = 0
    
    count_yearwise_0_to_2010 = 0
    count_yearwise_2011_to_2013 = 0
    count_yearwise_2014_to_2016 = 0
    count_yearwise_2017_to_2019 = 0
    
    
    count_oa_2007 = 0
    count_oa_2008 = 0
    count_oa_2009 = 0
    count_oa_2010 = 0
    count_oa_2011 = 0
    count_oa_2012 = 0
    count_oa_2013 = 0
    count_oa_2014 = 0
    count_oa_2015 = 0
    count_oa_2016 = 0
    count_oa_2017 = 0
    count_oa_2018 = 0
    
    
    count_all_2008 = 0
    count_all_2009 = 0
    count_all_2010 = 0
    count_all_2011 = 0
    count_all_2012 = 0
    count_all_2013 = 0
    count_all_2014 = 0
    count_all_2015 = 0
    count_all_2016 = 0
    count_all_2017 = 0
    count_all_2018 = 0
    
    
    for univ_name,univ_oa_details in val.items():
        count_country_OA_papers = count_country_OA_papers + univ_oa_details['count_OA_papers']
        count_country_unknown_papers = count_country_unknown_papers + univ_oa_details['count_unknown_papers']
        
        # Lets get the sum of count of OA in the selected intervals
        count_yearwise_0_to_2010 = count_yearwise_0_to_2010 + univ_oa_details["yearwise_OA"]["count_intervals"]["0-2010"]
        count_yearwise_2011_to_2013 = count_yearwise_2011_to_2013 + univ_oa_details["yearwise_OA"]["count_intervals"]["2011-2013"]
        count_yearwise_2014_to_2016 = count_yearwise_2014_to_2016 + univ_oa_details["yearwise_OA"]["count_intervals"]["2014-2016"]
        count_yearwise_2017_to_2019 = count_yearwise_2017_to_2019 + univ_oa_details["yearwise_OA"]["count_intervals"]["2017-2019"]
        
        # The sum of count of oa in specific years -- needed to find growth rate
        count_oa_2007 = count_oa_2007 + univ_oa_details["yearwise_OA"]["count_year"]["2007"]
        count_oa_2008 = count_oa_2008 + univ_oa_details["yearwise_OA"]["count_year"]["2008"]
        count_oa_2009 = count_oa_2009 + univ_oa_details["yearwise_OA"]["count_year"]["2009"]
        count_oa_2010 = count_oa_2010 + univ_oa_details["yearwise_OA"]["count_year"]["2010"]
        count_oa_2011 = count_oa_2011 + univ_oa_details["yearwise_OA"]["count_year"]["2011"]
        count_oa_2012 = count_oa_2012 + univ_oa_details["yearwise_OA"]["count_year"]["2012"]
        count_oa_2013 = count_oa_2013 + univ_oa_details["yearwise_OA"]["count_year"]["2013"]
        count_oa_2014 = count_oa_2014 + univ_oa_details["yearwise_OA"]["count_year"]["2014"]
        count_oa_2015 = count_oa_2015 + univ_oa_details["yearwise_OA"]["count_year"]["2015"]
        count_oa_2016 = count_oa_2016 + univ_oa_details["yearwise_OA"]["count_year"]["2016"]
        count_oa_2017 = count_oa_2017 + univ_oa_details["yearwise_OA"]["count_year"]["2017"]
        count_oa_2018 = count_oa_2018 + univ_oa_details["yearwise_OA"]["count_year"]["2018"]
        
        
        # The sum of count of all papers in specific years -- needed to find oa percent within each year
        count_all_2008 = count_all_2008 + univ_oa_details["yearwise_all"]["count_year"]["2008"]
        count_all_2009 = count_all_2009 + univ_oa_details["yearwise_all"]["count_year"]["2009"]
        count_all_2010 = count_all_2010 + univ_oa_details["yearwise_all"]["count_year"]["2010"]
        count_all_2011 = count_all_2011 + univ_oa_details["yearwise_all"]["count_year"]["2011"]
        count_all_2012 = count_all_2012 + univ_oa_details["yearwise_all"]["count_year"]["2012"]
        count_all_2013 = count_all_2013 + univ_oa_details["yearwise_all"]["count_year"]["2013"]
        count_all_2014 = count_all_2014 + univ_oa_details["yearwise_all"]["count_year"]["2014"]
        count_all_2015 = count_all_2015 + univ_oa_details["yearwise_all"]["count_year"]["2015"]
        count_all_2016 = count_all_2016 + univ_oa_details["yearwise_all"]["count_year"]["2016"]
        count_all_2017 = count_all_2017 + univ_oa_details["yearwise_all"]["count_year"]["2017"]
        count_all_2018 = count_all_2018 + univ_oa_details["yearwise_all"]["count_year"]["2018"]
    
    
    total_country_papers = count_country_OA_papers + count_country_unknown_papers
    percent_OA_country = (count_country_OA_papers * 100.00)/total_country_papers
    percent_unknown_country = (count_country_unknown_papers * 100.00)/total_country_papers
    
    countries_oa_info[key] = {}
    countries_oa_info[key]['count_OA_papers'] = count_country_OA_papers
    countries_oa_info[key]['count_unknown_papers'] = count_country_unknown_papers    
    countries_oa_info[key]['percent_OA_papers'] = percent_OA_country
    countries_oa_info[key]['percent_unknown_papers'] = percent_unknown_country
    countries_oa_info[key]['count_total_papers'] = total_country_papers
    
    countries_oa_info[key]["yearwise_OA"] = {}
    
    countries_oa_info[key]["yearwise_OA"]["count_intervals"] = {"0-2010": count_yearwise_0_to_2010,
                                                                "2011-2013": count_yearwise_2011_to_2013,
                                                               "2014-2016": count_yearwise_2014_to_2016,
                                                                "2017-2019": count_yearwise_2017_to_2019}
    
    
    
    
    countries_oa_info[key]["yearwise_OA"]["count_year"] = {"2007":count_oa_2007, "2008":count_oa_2008,
                                                           "2009":count_oa_2009, "2010":count_oa_2010,
                                                           "2011":count_oa_2011, "2012":count_oa_2012,
                                                           "2013":count_oa_2013, "2014":count_oa_2014,
                                                           "2015":count_oa_2015, "2016":count_oa_2016,
                                                           "2017":count_oa_2017, "2018":count_oa_2018} 
    
    # Lets find the percentage OA in each year
    countries_oa_info[key]["yearwise_OA"]["percent_year"] = {
        "2008":(count_oa_2008*100.00)/count_all_2008,
       "2009":(count_oa_2009*100.00)/count_all_2009,
        "2010":(count_oa_2010*100.00)/count_all_2010,
       "2011":(count_oa_2011*100.00)/count_all_2011,
        "2012":(count_oa_2012*100.00)/count_all_2012,
       "2013":(count_oa_2013*100.00)/count_all_2013,
        "2014":(count_oa_2014*100.00)/count_all_2014,
       "2015":(count_oa_2015*100.00)/count_all_2015, 
        "2016":(count_oa_2016*100.00)/count_all_2016,
       "2017":(count_oa_2017*100.00)/count_all_2017,
        "2018":(count_oa_2018*100.00)/count_all_2018
    }
    
    
    
    # Lets also find the growth within each intervals
    growth_rate = {}  # change divided by the time it took to make that change
    growth_rate["2008"] = (count_oa_2008 - count_oa_2007)/1.00
    growth_rate["2009"] = (count_oa_2009 - count_oa_2008)/1.00
    growth_rate["2010"] = (count_oa_2010 - count_oa_2009)/1.00
    growth_rate["2011"] = (count_oa_2011 - count_oa_2010)/1.00
    growth_rate["2012"] = (count_oa_2012 - count_oa_2011)/1.00
    growth_rate["2013"] = (count_oa_2013 - count_oa_2012)/1.00
    growth_rate["2014"] = (count_oa_2014 - count_oa_2013)/1.00
    growth_rate["2015"] = (count_oa_2015 - count_oa_2014)/1.00
    growth_rate["2016"] = (count_oa_2016 - count_oa_2015)/1.00
    growth_rate["2017"] = (count_oa_2017 - count_oa_2016)/1.00
    growth_rate["2018"] = (count_oa_2018 - count_oa_2017)/1.00
    countries_oa_info[key]["yearwise_OA"]["growth_rate"] = growth_rate
    
    
    
    
    
    countries_oa_percents[key] = percent_OA_country


# In[25]:


with open(join(output_dir,'all_countries_OA_info.txt'), 'w') as file:
     file.write(json.dumps(countries_oa_info, sort_keys=True, indent=4, ensure_ascii=False)) 


# In[26]:


countries_oa_percents


# In[27]:


countries_oa_percent_bar_plot = create_OA_percent_bar_chart(countries_oa_percents, save_fname = join(output_dir,"all_countries_OA_percent"), x_label = "Countries", display_values=True)


# In[28]:


countries_oa_percent_bar_plot


# In[ ]:





# In[29]:


def create_triple_bar_chart(oa_info_dict, save_fname, x_label=None):
    '''
    Contains plot of count OA, count Unknown and count Total. The % OA value is too small for this plot and is therefore separately shown in the other graph.
    '''
    
    #  Sort by Percentage OA   
    #  https://stackoverflow.com/a/37266356/530399
    sort_by_vals = sorted(oa_info_dict.items(), key=lambda kv: kv[1]['percent_OA_papers'], reverse=True) # sorted by values, return a list of tuples
    
    names = [x for (x,y) in sort_by_vals]
    
    # set width of bar
    barWidth = 0.3
    
    
    y_values_scale_down_factor = 10000
    # set height of bar -- scale down by 10k   
    bars_oa_count = [y['count_OA_papers']/y_values_scale_down_factor for (x,y) in sort_by_vals]
    bars_unknown_count = [y['count_unknown_papers']/y_values_scale_down_factor for (x,y) in sort_by_vals]
    bars_total_count = [(y['count_OA_papers']+y['count_unknown_papers'])/y_values_scale_down_factor for (x,y) in sort_by_vals]
    
    
    sep_between_group_bars = 2
    # Set position of bar on X axis
    r1 = np.arange(len(bars_oa_count))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]

    # Make the plot
    plt.figure(figsize=(15,10))
    
    plt.bar(r1, bars_oa_count, color='blue', width=barWidth, edgecolor='white', label='Count OA')
    plt.bar(r2, bars_unknown_count, color='red', width=barWidth, edgecolor='white', label='Count Unknown')
    plt.bar(r3, bars_total_count, color='green', width=barWidth, edgecolor='white', label='Total Papers Count')

    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label)
    ax.set_ylabel("Count of OA, Unknown and Total Papers (Expressed in terms of 10K) ")
    
    #     https://stackoverflow.com/a/8482667/530399
#     plt.text(0.7, 0.9,"Numbers are in terms of 10K", ha='center', va='center', transform=ax.transAxes)
    
    for p in ax.patches:
        h = p.get_height()
        x = p.get_x()+p.get_width()/2.
        ax.annotate("%g" % h, xy=(x,h), xytext=(0,4), rotation=90,  textcoords="offset points", ha="center", va="bottom")
    
    
    # Add xticks on the middle of the group bars
    plt.xticks([r + barWidth for r in range(len(bars_oa_count))], names, rotation='vertical')
    
    
    # Create legend & Show graphic
    plt.legend()

    
    plt.savefig(save_fname+".png", bbox_inches='tight')
    plt.savefig(save_fname+".pdf", bbox_inches='tight')
    
    
    plt.close()
    
    return ax.get_figure()


# In[30]:


countries_oa_info_bar_plot = create_triple_bar_chart(countries_oa_info, save_fname = join(output_dir,"all_countries_OA_info"), x_label = "Countries")


# In[31]:


countries_oa_info_bar_plot


# In[32]:


def create_OA_growth_line_chart(countries_oa_info, save_fname, x_label = "Year", plt_text=None):
    
    plt.figure(figsize=(15,10))
    
    country_names_list = []
    markers = ['o', 'x', 'v', 's', '*', '+', 'D', '|']
    for country_name,oa_info in countries_oa_info.items():
        
        growth_rate = oa_info["yearwise_OA"]["growth_rate"]
        # sort by year
        #  https://stackoverflow.com/a/37266356/530399
        sort_by_year = sorted(growth_rate.items(), key=lambda kv: int(kv[0]))
        years, growth_rates = zip(*sort_by_year) # unpack a list of pairs into two tuples
        
        plt.plot(years,growth_rates, linewidth=4, markersize=12, marker=markers[len(country_names_list)])
        
        country_names_list.append(country_name)
        
        
    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label)
    ax.set_ylabel("Growth Rate of OA papers per year")
    
    if plt_text:
#     https://stackoverflow.com/a/8482667/530399
        plt.text(0.7, 0.9,plt_text, ha='center', va='center', transform=ax.transAxes)
    
#     plt.xticks(years)
    plt.legend(country_names_list, loc='upper left')
    
    plt.savefig(save_fname+".png", bbox_inches='tight')
    plt.savefig(save_fname+".pdf", bbox_inches='tight')
    
    plt.close()
    
    return ax.get_figure()


# In[33]:


countries_OA_growth_line_plot = create_OA_growth_line_chart(countries_oa_info, save_fname = join(output_dir,"all_countries_OA_growth"), x_label = "Year")


# In[34]:


countries_OA_growth_line_plot


# In[ ]:





# In[35]:


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
        
        plt.plot(years,percent_oas, linewidth=4, markersize=12, marker=markers[len(country_names_list)])
        
        country_names_list.append(country_name)
        
        
    
    ax = plt.gca()
    if x_label:
        ax.set_xlabel(x_label)
    ax.set_ylabel("% of OA paper published in each year")
    
    if plt_text:
#     https://stackoverflow.com/a/8482667/530399
        plt.text(0.7, 0.9,plt_text, ha='center', va='center', transform=ax.transAxes)
    
#     plt.xticks(years)
    plt.legend(country_names_list, loc='upper left')
    
    plt.savefig(save_fname+".png", bbox_inches='tight')
    plt.savefig(save_fname+".pdf", bbox_inches='tight')
    
    plt.close()
    
    return ax.get_figure()


# In[36]:


countries_OA_percent_each_year_line_plot = create_yearwise_OA_percent_line_chart(countries_oa_info, save_fname = join(output_dir,"all_countries_OA_percent_each_year"), x_label = "Year")


# In[37]:


countries_OA_percent_each_year_line_plot


# In[ ]:





# In[ ]:





# In[ ]:





# In[38]:


countries_oa_info['brazil']


# In[ ]:





# In[39]:


print("\n\n\nCompleted!!!")


# In[ ]:





# In[ ]:




