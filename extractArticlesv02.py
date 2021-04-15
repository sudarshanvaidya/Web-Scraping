# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 19:22:13 2020

@author: DELL
"""

from bs4 import BeautifulSoup
import requests
import pandas as pd 
import json
import re
import time

start_time = time.time()

# Set restart point, if applicable.
restart = 0

# Initiate dataframe 
df_medium = pd.DataFrame(columns = ['URL','Uslug', 'Title', 'Description', 
                                    'Author', 'Date', 'Taglist', 'Lang', 'Claps', 'Text'])
# Get URL info into dataset 
# Dataset created from extractArchivev02.py 
df_url = pd.read_csv("C:/Users/DELL/AIML/14Scraping/allurls_data-science_1jan2018-31aug2020.csv")
df_url.head()

#url = "https://medium.com/analytics-vidhya/top-9-industry-natural-language-processing-applications-6d51a3031a84"
#url = "https://medium.com/ai-space/an-introductory-guide-to-chatbot-in-nlp-e30ff4951cce"
#url = "https://towardsdatascience.com/an-overview-of-model-compression-techniques-for-deep-learning-in-space-3fd8d4ce84e5"

for n in range(restart, len(df_url)):
    url = df_url["First"][n]
    try: 
        resp = requests.get(url)
    except Exception: 
        pass
    if resp.content is None:
        print("None response data for URL",url)
        continue
    try: 
        soup = BeautifulSoup(resp.content, 'html5lib')
    
        data = soup.find("script",type="application/ld+json")
        if data is None: 
            print("None data for URL",url)
            continue
        js = json.loads(data.text)
    
        title = js["name"]
        description = js["description"]
        identifier = js["identifier"]
        creator = js["creator"][0]
        dt = js["dateCreated"].split("T")[0]
        print(dt)
        keywords = js["keywords"]
        taglist = []
        for tag in keywords:
            if 'Tag:' in tag: taglist.append(tag.lstrip('Tag:'))
            #print(taglist)
        a_text = soup.find_all('p')
        text = [re.sub(r'<.+?>',r'',str(a)) for a in a_text]
        #print(text)
    
        df_medium = df_medium.append({'URL' : url, 
                                  'Uslug' : df_url['Uslug'][n], 
                                  'Title' : title, 
                                  'Description' : description, 
                                  'Author' : creator, 'Date' : dt, 
                                  'Taglist' : taglist, 
                                  'Lang' : df_url['detectedLanguage'][n], 
                                  'Claps' : df_url['totalClapCount'][n], 
                                  'Text' : str(text)}, ignore_index=True)
    except Exception: 
        pass    
    # Introduce delay to reduce traffic burden on the server 
    time.sleep(1)
    
    # Download to CSV regularly to prevent loss of data on errors 
    if (n%500)==0: 
        filename = "C:/Users/DELL/AIML/14Scraping/medium_data-science_" + str(n) + ".csv"
        df_medium.to_csv(filename)
    
df_medium.to_csv("C:/Users/DELL/AIML/14Scraping/medium_data-science_1jan2018-31aug2020.csv")
print("--- %s seconds ---" % (time.time() - start_time)) 




