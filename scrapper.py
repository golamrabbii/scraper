import json
import pandas as pd
import time
import re
from urllib.request import urlopen
from fuzzywuzzy import fuzz
import requests
import os
import csv
import subprocess

n=1
#read search keyword from keyword.csv scrape data based on those keyword

df = pd.read_csv('keyword.csv')
for index,i in df.iterrows():
    keyword = i['keyword']
    name=[]
    address = []
    phone = []
    long, lat = [], []
    place,lt,lng = [],[],[]

    #facebook graph API url

    url ="https://graph.facebook.com/v4.0/search?access_token=EAAGyb8vyaYABAEZBEWMg13tFMosZAbDA1xJfeMlZCDniBQNADEkZCtnb6Udw5jQn9HUJmiNgYDVEBtcRJx6D2ZABiBmbocDmOpxqgRVZAdBM4Pz6XcZARkmaZBTdfItXjVMc5ZAB0n2TmncUjhR6WUG3ZC9txCKZBAZBn9lyCtlOjp3bJmEKk5aaNrU4&pretty=0&fields=name%2Cphone%2Clocation&q="+keyword+"+in+dhaka&type=place&limit=100"
    facebook_connection = urlopen(url)
    data = facebook_connection.read().decode('utf8')
    json_object = json.loads(data)
    post1 = json_object["data"]

    for j in post1:
        nam = j['name']
        name.append(nam)
        loc = j['location']
        long.append(loc['longitude'])
        lat.append(loc['latitude'])
        lng.append(loc['longitude'])
        lt.append(loc['latitude'])
        if 'street' in loc:
            place.append(loc['street'])
        else:
            place.append("Null")
        
    df = pd.DataFrame()
    df['name']= name
    df['longitude'] = long
    df['latitude'] = lat
    df['Address'] = place

    #save collected data to a csv file
    
    if n==1:
        df.to_csv("scrapper.csv", header=True,index=False)
        n=2
    else:
        with open('scrapper.csv', 'a') as f:
            df.to_csv(f, header=None, index=False)

    if 'paging' in json_object:
        
        print("if")
        cursor = json_object["paging"]
        st = cursor['cursors']
        if 'after' in st:
            str1 = st['after']
        else:
            str1 = ""
        while True:

            if str1:
                url2 = url+"&after="+str1
                time.sleep(2)
                facebook_connection = urlopen(url2)
                data2 = facebook_connection.read().decode('utf8')
                json_object = json.loads(data2)
                post2 = json_object["data"]
                cursor2 = json_object["paging"]
                st = cursor2['cursors']
                if 'after' in st:
                    str1 = st['after']
                else:
                    str1=""
                df2 = pd.DataFrame()
                for k in post2:
                    name.append(k['name'])
                    loc = k['location']
                    long.append(loc['longitude'])
                    lat.append(loc['latitude'])
                    lng.append(loc['longitude'])
                    lt.append(loc['latitude'])
                    if 'street' in loc:
                        place.append(loc['street'])
                    else:
                        place.append("Null")
                df2['name'] = name
                df2['longitude'] = long
                df2['latitude'] = lat
                df2['Address'] = place
                with open('scrapper.csv','a') as f:
                    df2.to_csv(f,header=None,index=False)
            else:
                break

#Remove duplicate data from scrapper.csv file

df1 = pd.read_csv("scrapper.csv")
df1.drop_duplicates(subset=None,keep='first',inplace=True)
df1.to_csv("scrapper.csv",index=False)

#After completed data scraping  it gives a message to slack incomming webhook automatically
      
url = "(incomming webhook url)"
msg = {'text':'Data Collected Successfully'}
#requests.post(url,data=json.dumps(msg))
