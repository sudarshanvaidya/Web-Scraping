# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 19:22:13 2020

@author: DELL
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd 
from datetime import datetime
from datetime import timedelta
import time

start_time = time.time()

# A function to accept an url and return list of child urls 
def getChildUrlData(url):
 
    # Getting the webpage, creating a Response object.
    response = requests.get(url)
 
    # Extracting the source code of the page.
    data = response.text
 
    # Passing the source code to BeautifulSoup to create a BeautifulSoup object for it.
    soup = BeautifulSoup(data, 'lxml')

    # Define dataframe for URLs 
    df_url = pd.DataFrame(columns = ['URL']) 
    #df_url_new = pd.DataFrame(columns = ['First', 'Last']) 
    df_url_filtered_unique = pd.DataFrame(columns = ['URL', 'First', 'Last', 'Uslug']) 
    #df_url_final = pd.DataFrame(columns = ['URL']) 
    # Extracting all the <a> tags into a list.
    tags = soup.find_all('a')
 
    # Extracting URLs from the attribute href in the <a> tags.
    for tag in tags:
        #print(tag.get('href'))
        df_url = df_url.append({'URL': tag.get('href')}, ignore_index=True)

    # Retain only the article links 
    #print(df_url.shape)
    df_url.drop(df_url[df_url['URL'].str.contains("#--responses")].index, inplace = True) 
    df_url_filtered = df_url[df_url['URL'].str.contains("tag_archive")] 
    df_url_filtered_unique['URL'] = df_url_filtered.URL.unique()
    # Cleanse the URL and add a separate column for unique slug
    for ind in df_url_filtered_unique.index: 
        c = df_url_filtered_unique['URL'][ind]
        a, b = c.split("?source")
        df_url_filtered_unique['First'][ind] = a 
        df_url_filtered_unique['Last'][ind] = b 
        df_url_filtered_unique['Uslug'][ind] = a.split("/")[-1]
    
    df_url_filtered_unique.drop_duplicates(subset ="Last", keep = 'last', inplace = True)
    #print(df_url_filtered_unique['First']+df_url_filtered_unique['Last'])
    df_url_filtered_unique.drop(columns=['URL', 'Last'], inplace = True)
    #print(df_url_filtered_unique.head)
    df_url_filtered_unique.to_csv("C:/Users/DELL/AIML/14Scraping/urls.csv")

    # Create new DF with fields - clap, lang and uslug 

    # Separate the JSON part of the response      
    mainsplit=data.split("obvInit")
    splitpre=mainsplit[4]
    #print(splitpre)
    splitpost=splitpre.split("// ]]></script>")
    middle=splitpost[0]
    struc=middle[3:]
    struc = struc.rstrip(')')
    struc = struc.replace('\\', '')
    #print(struc) 
    
    # Isolate totalClapCount, detectedLanguage and Uslug 
    clapcount = []
    language = []
    unislug = []
    clap = struc.split("\"totalClapCount\":")
    for i in range(1, len(clap)):
        clapcount.append(clap[i].split(",")[0])

    lang = struc.split("\"detectedLanguage\":\"")
    for i in range(1, len(lang)):
        language.append(lang[i].split("\"")[0])

    uslug = struc.split("\"uniqueSlug\":\"")
    for i in range(1, len(uslug)):
        unislug.append(uslug[i].split("\"")[0])

    #print(clapcount[1:10])
    #print(language[1:10])
    #print(unislug[1:10])
    # Create DataFrame with these three lists 
    dict = {'totalClapCount': clapcount, 'detectedLanguage': language, 'Uslug': unislug}  
    df_info = pd.DataFrame(dict) 
    #df_info = pd.DataFrame(list(zip(clapcount, language, uslug)), 
    #           columns =['totalClapCount', 'detectedLanguage', 'Uslug']) 
    #print(df_info.info)
    #print(df_url_filtered_unique.info)
    #df_info.to_csv("C:/Users/DELL/AIML/14Scraping/info.csv")

    # Here merge df_info with df_url_filtered_unique on Uslug
    df = pd.merge(df_url_filtered_unique, df_info, on='Uslug', how='inner')
    #print(df.info)

    return df



# Initiate date range to be considered while scraping 
start_date = '20180101'
end_date = '20200831' 

# Process date range 
startDate=datetime.strptime(start_date,"%Y%m%d")
endDate=datetime.strptime(end_date,"%Y%m%d")
delta=endDate-startDate
#print(delta)

# Initiate tags to be considered while scraping 
#tags = ['data-science', 'AI', 'artificial-intelligence', 'machine-learning']
tags = ['data-science']
start_urls =[]
for tagSlug in tags: 
    start_urls.append(['https://medium.com/tag/'+tagSlug.strip("'")+'/archive/'])
#print(start_urls)

all_urls = pd.DataFrame(columns = ['First']) 
df_child_urls = pd.DataFrame(columns = ['First']) 
for i in range(delta.days + 1):
    d=datetime.strftime(startDate+timedelta(days=i),'%Y/%m/%d')
    for url in start_urls:
        string = str(url)
        string = string.strip("[")
        string = string.strip("]")
        string = string.strip("'")
        string = string + d
        print(string)
        all_urls = getChildUrlData(string)
        df_child_urls = df_child_urls.append(all_urls)
        #print(all_urls.shape)
        #print(df_child_urls.shape)

df_child_urls.drop_duplicates(subset ="First", keep = 'last', inplace = True)
df_child_urls.to_csv("C:/Users/DELL/AIML/14Scraping/allurls.csv")
print("--- %s seconds ---" % (time.time() - start_time)) 
