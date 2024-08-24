# Databricks notebook source
from os import listdir, getenv
from urllib.request import urlopen
from bs4  import BeautifulSoup
import requests
import datetime
import io

# COMMAND ----------

df = spark.sql('select max(Day) from silver.events_export')
extraction_start_date = df.collect()[0][0]

# COMMAND ----------

def send_data(item:str) -> None:
    base_url = 'http://data.gdeltproject.org/events/'
    doc_name = item.split('.')
    path = doc_name.pop(1)
    doc_name = '.'.join(doc_name)
    req = requests.get(base_url + item)

    with open('/dbfs/tmp/'+doc_name.lower(), 'wb') as f:
        f.write(req.content)
    
    print(item)
    dbutils.fs.mv('/tmp/'+doc_name.lower(), f'/mnt/landing/events/{path}/{doc_name.lower()}')


# COMMAND ----------

url = 'http://data.gdeltproject.org/events/index.html'
text_soup = BeautifulSoup(urlopen(url).read()) #read in
lines = text_soup.find_all('a', href=True)

## Filter data
extract_list = [l['href'] for l in lines if 'CSV' in l['href'] and (int(l['href'].split('.')[0]) > extraction_start_date )]

# COMMAND ----------

globalHeader = ['GlobalEventID','Day','MonthYear','Year','FractionDate']
actorsHeader = ['Actor1Code','Actor1Name','Actor1CountryCode','Actor1KnownGroupCode','Actor1EthnicCode','Actor1Religion1Code','Actor1Religion2Code','Actor1Type1Code','Actor1Type2Code','Actor1Type3Code'] # Actor2
actors2Header = [l.replace('Actor1','Actor2') for l in actorsHeader]
eventHeader = ['IsRootEvent','EventCode','EventBaseCode','EventRootCode','QuadClass','GoldsteinScale','NumMentions','NumSources','NumArticles','AvgTone']
eventGeoHeader = ['Actor1Geo_Type','Actor1Geo_Fullname','Actor1Geo_CountryCode','Actor1Geo_ADM1Code','Actor1Geo_Lat','Actor1Geo_Long','Actor1Geo_FeatureID'] # Actor 2 e Action
eventGeo2Header = [l.replace('Actor1','Actor2') for l in eventGeoHeader]
eventGeoAcHeader = [l.replace('Actor1','Action') for l in eventGeoHeader]
dataManagHeader = ['DATEADDED','SOURCEURL']

header = globalHeader + actorsHeader + actors2Header + eventHeader + eventGeoHeader + eventGeo2Header + eventGeoAcHeader + dataManagHeader

# COMMAND ----------


for i in extract_list:
    try:
        send_data(i)
    except Exception as e:
        print('Error reading ' + e)
