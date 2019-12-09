#!/usr/bin/env python
# coding: utf-8

# # Distribution of seniority (number of years since first publication until last publication) of the staff at a particular university.

# In[1]:


# standard path wrangling to be able to import project config and sources
import os
import sys
root = os.path.dirname(os.getcwd())  # One can walk up the directory tree by calling os.path.dirname as many times as needed.
sys.path.append(root)
print('Project root: {}'.format(root))


# Imports:

# In[2]:


get_ipython().system('ls ..')


# In[3]:


# Built-in
import csv
import json
from os.path import join as pjoin

# Installed
import pandas as pd
from tqdm import tqdm
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from datetime import date
import math


# Other setup:

# In[4]:


sns.set()
palette = sns.color_palette('muted')
sns.set_palette(palette)

pd.options.display.float_format = '{:.2f}'.format
pd.set_option('max_colwidth', 800)
pd.set_option('display.max_rows', 1000)


# In[ ]:





# In[5]:


def get_author_seniority_years_count(univ_df):
    authors_seniority = []
    for author_id in univ_df.authorid.unique():
        author_df = univ_df[univ_df['authorid']==author_id]
        author_dates_df = author_df.loc[:,['date']]
        
        author_min_date = author_dates_df.date.min()
        author_max_date = author_dates_df.date.max()
        # https://stackoverflow.com/a/151211
        delta = author_max_date - author_min_date
        author_seniority_years = delta.days/365.00 # delta doesn't have years attribute
        authors_seniority.append(author_seniority_years)
    return authors_seniority


# In[6]:


def get_all_univ_names_of_country(q4_df_copy, country_name):
    country_df = q4_df_copy[q4_df_copy["country_name"]==country_name]
    return country_df["institution_name"].unique()


# In[7]:


def get_buckets_counts(univ_author_seniority_years):
    count_0_to_5 = 0
    count_5dot1_to_10 = 0
    count_10dot1_to_15 = 0
    count_15dot1_to_20 = 0
    count_20dot1_to_25 = 0
    count_25dot1_to_30 = 0
    count_over_30 = 0
    for x in univ_author_seniority_years:
        if x>0 and x<=5:
            count_0_to_5 = count_0_to_5 + 1
        if x>5 and x<=10:
            count_5dot1_to_10 = count_5dot1_to_10 + 1
        if x>10 and x<=15:
            count_10dot1_to_15 = count_10dot1_to_15 + 1
        if x>15 and x<=20:
            count_15dot1_to_20 = count_15dot1_to_20 + 1
        if x>20 and x<=25:
            count_20dot1_to_25 = count_20dot1_to_25 + 1
        if x>25 and x<=30:
            count_25dot1_to_30 = count_25dot1_to_30 + 1
        if x>30:
            count_over_30 = count_over_30 + 1
    return [count_0_to_5,count_5dot1_to_10,count_10dot1_to_15,count_15dot1_to_20,count_20dot1_to_25,count_25dot1_to_30,count_over_30]


# In[8]:


def add_bar_plot_for_country(univs_author_seniority_copy, country_all_univs_names, country_name):
    # Copied from http://python-graph-gallery.com/11-grouped-barplot/ and https://python-graph-gallery.com/13-percent-stacked-barplot/
    
    # set width of bar
    barWidth = 0.10

    
    bucket1_values = []  # Each element here is the count of bucket1 values from a distinct university
    bucket2_values = [] # Each element here is the count of bucket2 values from the corresponding university
    bucket3_values = [] # Each element here is the count of bucket3 values from the corresponding university
    bucket4_values = [] # Each element here is the count of bucket4 values from the corresponding university
    bucket5_values = [] # Each element here is the count of bucket5 values from the corresponding university
    bucket6_values = [] # Each element here is the count of bucket6 values from the corresponding university
    bucket7_values = [] # Each element here is the count of bucket7 values from the corresponding university
    
    for univ_name in country_all_univs_names:
        count_buckets = get_buckets_counts(univs_author_seniority_copy[univ_name])
        bucket1_values.append(count_buckets[0])
        bucket2_values.append(count_buckets[1])
        bucket3_values.append(count_buckets[2])
        bucket4_values.append(count_buckets[3])
        bucket5_values.append(count_buckets[4])
        bucket6_values.append(count_buckets[5])
        bucket7_values.append(count_buckets[6])
        
    
    
    
    
    # From raw values to percentage
    raw_data = {'<=5':bucket1_values, '>5 and <=10': bucket2_values, '>10 and <=15': bucket3_values, '>15 and <=20': bucket4_values, '>20 and <=25': bucket5_values, '>25 and <=30': bucket6_values, '>30': bucket7_values}
    raw_data_df = pd.DataFrame(raw_data)
    totals = [a+b+c+d+e+f+g for a,b,c,d,e,f,g in zip(raw_data_df['<=5'], raw_data_df['>5 and <=10'], raw_data_df['>10 and <=15'], raw_data_df['>15 and <=20'], raw_data_df['>20 and <=25'], raw_data_df['>25 and <=30'], raw_data_df['>30'])]
    first_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['<=5'], totals)]
    second_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>5 and <=10'], totals)]
    third_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>10 and <=15'], totals)]
    forth_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>15 and <=20'], totals)]
    fifth_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>20 and <=25'], totals)]
    sixth_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>25 and <=30'], totals)]
    last_bucket_bars = [i / j * 100 for i,j in zip(raw_data_df['>30'], totals)]
    
    
    
    # Set position of bar on X axis
    r1 = np.arange(len(first_bucket_bars))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    r4 = [x + barWidth for x in r3]
    r5 = [x + barWidth for x in r4]
    r6 = [x + barWidth for x in r5]
    r7 = [x + barWidth for x in r6]

    
    
    # Create plots
    plt.bar(r1, first_bucket_bars, color='dodgerblue', edgecolor='white', width=barWidth, label="<=5 years")
    plt.bar(r2, second_bucket_bars, color='purple', edgecolor='white', width=barWidth, label=">5 and <=10 years")
    plt.bar(r3, third_bucket_bars, color='limegreen', edgecolor='white', width=barWidth, label=">10 and <=15 years")
    plt.bar(r4, forth_bucket_bars, color='yellow', edgecolor='white', width=barWidth, label=">15 and <=20 years")
    plt.bar(r5, fifth_bucket_bars, color='darkkhaki', edgecolor='white', width=barWidth, label=">20 and <=25 years")
    plt.bar(r6, sixth_bucket_bars, color='orange', edgecolor='white', width=barWidth, label=">25 and <=30 years")
    plt.bar(r7, last_bucket_bars, color='red', edgecolor='white', width=barWidth, label=">30 years")
    
    # plt.bar(oa_bars, color='#7f6d5f', width=barWidth, edgecolor='white', label='OA')
    # plt.bar(unknown_bars, color='#557f2d', width=barWidth, edgecolor='white', label='unknown')


    # Custom x axis
    plt.xticks([r + barWidth for r in range(len(first_bucket_bars))], country_all_univs_names, fontweight='bold')
    plt.xlabel(country_name.upper(), fontweight='bold', color='deeppink', size=20)
    
    plt.yticks([0,20,40,60,80,100], [0,20,40,60,80,100])

    plt.ylabel("% Authors Count", fontweight='bold', color='deeppink', size=20)

    # Add a legend
    plt.legend(loc='upper left', bbox_to_anchor=(1,1), ncol=1)


# In[ ]:





# In[9]:


q4_df = pd.read_csv(pjoin(root, "data/raw/dataset_question4/all_authors.csv"), sep=",", header=0)
print(q4_df.shape)


# In[10]:


q4_df.head(400000).tail(10)


# In[11]:


print(q4_df.info())


# In[12]:


# Convert the date column which is a string(strings are denoted as object type in Python)
q4_df['date'] =  pd.to_datetime(q4_df['date'], format='%Y-%m-%d')

# Confirm the date column is in datetime format
print(q4_df.info())


# In[13]:


# Now we can retrieve the individual bits of date -- month, year and day
'''
print(q4_df['date'][399992].year)
print(q4_df['date'][399992].month)
print(q4_df['date'][399992].day)
'''


# In[14]:


univs_author_seniority_stats = {}
for univ_name in q4_df.institution_name.unique():
    print(univ_name)
    univ_df = q4_df[q4_df["institution_name"]==univ_name]
    result_univ_authors_seniority = get_author_seniority_years_count(univ_df)
#     print(result_univ_authors_seniority)
    univs_author_seniority_stats[univ_name] = result_univ_authors_seniority
    print("\n")


# # Country Wise Plot

# In[15]:


country_names = q4_df.country_name.unique()
plot_total_rows_count = len(country_names)
plot_total_cols_count = max(len(get_all_univ_names_of_country(q4_df,country_name)) for country_name in country_names)
print(plot_total_rows_count, plot_total_cols_count)


# In[16]:


fig = plt.figure(figsize=(44, len(country_names)*5), dpi=180)
# plt.subplots_adjust(hspace = 1.5)  # the vertical distance betweeen two subplots

# plot_row_nb = 0
plot_nb = 1

for country_name in country_names:
    plt.subplot(len(country_names),1, plot_nb)  # First coloumn of every row; the plot for a new country.
    country_all_univs_names = get_all_univ_names_of_country(q4_df,country_name)
    print(country_name + ": " + str(country_all_univs_names))
    add_bar_plot_for_country(univs_author_seniority_stats, country_all_univs_names, country_name)
#     add_bar_plot_for_country(plot_total_rows_count, plot_total_cols_count, plot_row_nb, univs_author_seniority_stats, country_all_univs_names, country_name)
    plot_nb = plot_nb + 1
#     plot_row_nb = plot_row_nb + 1
    print("\n")
    
# fig.suptitle('% Distribution of seniority (number of years since their first publication until last publication) of staff at universities')


plt.savefig(pjoin(root, "documents/dataset_question4/ds_q4_analysis.png"), bbox_inches='tight')
plt.savefig(pjoin(root, "documents/dataset_question4/ds_q4_analysis.pdf"), bbox_inches='tight')

plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




