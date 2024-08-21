# Databricks notebook source
import pandera as pa
import pandas as pd
from os import listdir, getenv
from pandera import Column, DataFrameSchema, Index
import threading
import numpy as np
from azure.storage.filedatalake import DataLakeServiceClient
from urllib.request import urlopen
from bs4  import BeautifulSoup
import requests
import datetime
import io

# COMMAND ----------

## Environment Variables
ST_KEY = getenv('MY_ENV_VAR')

## Set Start Date
extraction_start_date = datetime.date.today().strftime('%Y%m%d')

# COMMAND ----------

schema = DataFrameSchema(
    {
        "GlobalEventID" : Column(np.int64),
        "Day" : Column(np.int64),
        "MonthYear" : Column(np.int64),
        "Year" : Column(np.int64),
        "FractionDate" : Column(np.float64),
        "Actor1Code" : Column(object, nullable=True),
        "Actor1Name" : Column(object, nullable=True),
        "Actor1CountryCode" : Column(object, nullable=True),
        "Actor1KnownGroupCode" : Column(object, nullable=True),
        "Actor1EthnicCode" : Column(object, nullable=True),
        "Actor1Religion1Code" : Column(object, nullable=True),
        "Actor1Religion2Code" : Column(object, nullable=True),
        "Actor1Type1Code" : Column(object, nullable=True),
        "Actor1Type2Code" : Column(object, nullable=True),
        "Actor1Type3Code" : Column(object, nullable=True),
        "Actor2Code" : Column(object, nullable=True),
        "Actor2Name" : Column(object, nullable=True),
        "Actor2CountryCode" : Column(object, nullable=True),
        "Actor2KnownGroupCode" : Column(object, nullable=True),
        "Actor2EthnicCode" : Column(object, nullable=True),
        "Actor2Religion1Code" : Column(object, nullable=True),
        "Actor2Religion2Code" : Column(object, nullable=True),
        "Actor2Type1Code" : Column(object, nullable=True),
        "Actor2Type2Code" : Column(object, nullable=True),
        "Actor2Type3Code" : Column(object, nullable=True),
        "IsRootEvent" : Column(np.int64, nullable=True),
        "EventCode" : Column(object, nullable=True),
        "EventBaseCode" : Column(object, nullable=True),
        "EventRootCode" : Column(object, nullable=True),
        "QuadClass" : Column(np.int64, nullable=True),
        "GoldsteinScale" : Column(np.float64, nullable=True),
        "NumMentions" : Column(np.int64, nullable=True),
        "NumSources" : Column(np.int64, nullable=True),
        "NumArticles" : Column(np.int64, nullable=True),
        "AvgTone" : Column(np.float64, nullable=True),
        "Actor1Geo_Type" : Column(np.int64, nullable=True),
        "Actor1Geo_Fullname" : Column(object, nullable=True),
        "Actor1Geo_CountryCode" : Column(object, nullable=True),
        "Actor1Geo_ADM1Code" : Column(object, nullable=True),
        "Actor1Geo_Lat" : Column(np.float64, nullable=True),
        "Actor1Geo_Long" : Column(np.float64, nullable=True),
        "Actor1Geo_FeatureID" : Column(object, nullable=True),
        "Actor2Geo_Type" : Column(np.int64, nullable=True),
        "Actor2Geo_Fullname" : Column(object, nullable=True),
        "Actor2Geo_CountryCode" : Column(object, nullable=True),
        "Actor2Geo_ADM1Code" : Column(object, nullable=True),
        "Actor2Geo_Lat" : Column(np.float64, nullable=True),
        "Actor2Geo_Long" : Column(np.float64, nullable=True),
        "Actor2Geo_FeatureID" : Column(object, nullable=True),
        "ActionGeo_Type" : Column(np.int64, nullable=True),
        "ActionGeo_Fullname" : Column(object, nullable=True),
        "ActionGeo_CountryCode" : Column(object, nullable=True),
        "ActionGeo_ADM1Code" : Column(object, nullable=True),
        "ActionGeo_Lat" : Column(np.float64, nullable=True),
        "ActionGeo_Long" : Column(np.float64, nullable=True),
        "ActionGeo_FeatureID" : Column(object, nullable=True),
        "DATEADDED" : Column(np.int64),
        "SOURCEURL" : Column(object)
    },
    index=Index(np.int64),
    strict=True,
    coerce=True,
)

# COMMAND ----------

class AzureDocuments:
    """Class to manage Blob Storage Operations"""
    def __init__(self, st_key, acc_name):
        """Start Class with blob Storage"""
        storage_account_key = st_key
        self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", acc_name), credential=storage_account_key)
        

    def upload_to_blob(self, blob_data: bytes, path:str, file_name:str, bucket_name:str)-> bool:
        """Upload Bytes to blob Storage"""

        file_system_client = self.service_client.get_file_system_client(file_system=bucket_name)
        directory_client = file_system_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)

        up = file_client.upload_data(blob_data, overwrite=True)
        return True
        
        

# COMMAND ----------

def send_data(list_items:list) -> None:
    base_url = 'http://data.gdeltproject.org/events/'
    for item in list_items:
        doc_name = item.split('.')
        path = doc_name.pop(1)
        doc_name = '.'.join(doc_name)
        req = requests.get(base_url + item)
        df = pd.read_csv(io.BytesIO(req.content), compression = 'zip', sep='\t', names =header,low_memory=False)
        try:
            schema.validate(df)
            data = io.BytesIO()
            df.to_parquet(data,  index=False,engine='pyarrow')
            # df.to_parquet('events/'+path+'/' + doc_name.lower().replace('csv.zip','parquet'),  index=False,engine='pyarrow') # Local Storage
            data.seek(0)
            az_docs.upload_to_blob(data.read(),'bronze/events/'+path,doc_name.lower().replace('csv','parquet'),'prod')
        except pa.errors.SchemaError as exc:
            print('error - %s' % exc)



# COMMAND ----------

url = 'http://data.gdeltproject.org/events/index.html'
text_soup = BeautifulSoup(urlopen(url).read()) #read in
lines = text_soup.find_all('a', href=True)

## Filter data
extract_list = [l['href'] for l in lines if 'CSV' in l['href'] and (int(l['href'].split('.')[0]) > extraction_start_date )]

# COMMAND ----------

az_docs = AzureDocuments(ST_KEY, 'datalakebifrostanalytics')

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

n = 10
data_chunck = [extract_list[i * n:(i + 1) * n] for i in range((len(extract_list) + n - 1) // n )]  
threads = []
for i in data_chunck:
    thread = threading.Thread(target=send_data, args=(i,))
    threads.append(thread)
    thread.start()
