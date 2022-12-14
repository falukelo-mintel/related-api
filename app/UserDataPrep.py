import pandas as pd
import glob
import os
import numpy as np
from tqdm import tqdm

import json
import ast
from pandas.io.json import json_normalize
from urllib.parse import unquote
import regex as re

import io
from io import StringIO
from io import BytesIO

from google.cloud import firestore
from google.cloud import storage
from google.cloud import logging
from google.cloud import bigquery

import google
import logging as log
import requests
from datetime import datetime
from datetime import date

from helper import *

def read_from_blob(bucket_name,file_name):
    
    #Use the top one for production TODO
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    #Use this one for different gcp environment
    # credentials = "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    # client = storage.Client(project='connect-x-production').from_service_account_json(credentials)
    # bucket = client.get_bucket(BUCKET_NAME)
    
    blob = bucket.blob(file_name)
    
    return blob.download_as_string().decode()

def CleanUserData():
    dateCol = ["createdDate"]
    
    #Initializing Client TODO
    ## HERE comment client with args when switching to production
    client = storage.Client()
    # client = storage.Client(project='connect-x-production').from_service_account_json(
    #         "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    #     )
    logging_client = logging.Client()
    logging_client.setup_logging()
    BUCKET_NAME = 'connect-x-production.appspot.com'
    bucket = client.get_bucket(BUCKET_NAME)
    
    print("Getting User Data")
    last_date = 'Organizes/pJoo5lLhhAbbofIfYdLz/AI/LastDate.txt'
    
    #Fulldf = pd.read_csv('gs://connectx-ai-backupdata-rd/krungsri/data/data_2505_2511.csv',parse_dates=dateCol) #Change this
    
    queryDate = read_from_blob(BUCKET_NAME,last_date)
    credentials = google.oauth2.service_account.Credentials.from_service_account_file(
                    '/home/jupyter/krungsri/connect-x-production-1a148ceb71e0.json',
                    scopes=['https://www.googleapis.com/auth/cloud-platform'])
    
    #TODO Uncomment credentials for production
    bqclient = bigquery.Client()
    # bqclient = bigquery.Client(credentials=credentials)
    
    sql = ( f"SELECT cx_cookie, unknownContact, createdDate, cx_event, cx_link "
            f"FROM `connect-x-production.stream_pJoo5lLhhAbbofIfYdLz.activities` "
            f"WHERE createdDate >= '{queryDate}' "
            f"AND (cx_event = 'Pageview' OR cx_event = 'View' OR cx_event = 'Click' OR cx_event = 'Drop Form' OR cx_event = 'Unknown to Known') "       
            )

    Fulldf = bqclient.query(sql).to_dataframe()
    
    Fulldf = Fulldf.set_index("createdDate").last("6M")
    #Fulldf.drop(columns=['Unnamed: 0'], inplace = True)
    Fulldf['cx_link'] = list(map(reconstructLink, tqdm(Fulldf['cx_link'].to_list())))

    Fulldf = Fulldf[list(map(includePPTCP,tqdm(Fulldf.cx_link.to_list())))]
    Fulldf = Fulldf[list(map(removeBank,tqdm(Fulldf.cx_link.to_list())))]
    Fulldf = Fulldf[list(map(removeWealth,tqdm(Fulldf.cx_link.to_list())))]
    Fulldf = Fulldf[list(map(removeJP,tqdm(Fulldf.cx_link.to_list())))]
    Fulldf = Fulldf[list(map(removeBusiness,tqdm(Fulldf.cx_link.to_list())))]
    print("Getting Scraped Data")
    credentials = "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    
    #COMMENT OUT storage options when in production TODO
    # df_scraped_all = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/ScrapedData/25_11_2022Full.csv",
    #                             storage_options={"token": credentials})
    df_scraped_all = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/ScrapedData/25_11_2022Full.csv")
    df_scraped_all.drop(columns = ['Unnamed: 0'], inplace = True)
    df_scraped_all['url'] = list(map(reconstructLink,tqdm(df_scraped_all['url'].to_list())))
    df_scraped_all.drop_duplicates(subset = ['url'],inplace = True)

    print("Getting Validated Data")
    # bucket = client.bucket(BUCKET_NAME)
    ValLinksblobs = bucket.list_blobs(prefix = "Organizes/pJoo5lLhhAbbofIfYdLz/AI/ValidatedLinks")
    ValFiles = [blob for blob in ValLinksblobs if 'csv' in blob.name]

    li_reLinks = []
    valPath = "gs://connect-x-production.appspot.com"
    for file in ValFiles:
        n_path = os.path.join(valPath, file.name)
        #COMMENT OUT storage options when in production TODO
        # new = pd.read_csv(n_path, storage_options={"token": credentials})
        new = pd.read_csv(n_path)
        li_reLinks.append(new)

    df_valLinks = pd.concat(li_reLinks, axis = 0)
    df_valLinks['url2'] = list(map(reconstructLink, tqdm(df_valLinks['url2'].to_list())))

    correspVal = dict()
    for key, val in zip(df_valLinks.url.to_list(),df_valLinks.url2.to_list()):
        correspVal[key] = val
    #__________________________________________________________________________
    Fulldf['cx_link'] = Fulldf.loc[:, 'cx_link'].map(correspVal).fillna(Fulldf['cx_link'])
    Fulldf['cx_link'] = list(map(reconstructLink,tqdm(Fulldf.cx_link.to_list())))

    Fulldf = Fulldf[Fulldf['cx_link'].isna() == False]
    Fulldf = Fulldf[Fulldf['cx_link']!= '']

    correspValScraped = dict()
    for key in df_scraped_all.url.to_list():
        correspValScraped[key] = 1.0
    #__________________________________________________________________________
    Fulldf['isMatch'] = Fulldf.loc[:, 'cx_link'].map(correspValScraped)

    Fulldf = Fulldf[Fulldf.isMatch.isna()==False]
    Fulldf.drop(columns = ['isMatch'],inplace = True)

    print("Finished Cleaning Data")
    today = date.today()
    userPath = "gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/CleanedUserData"
    filename_User = userPath+"/CleanedUser_"+today.strftime("%Y%m%d")+".csv"

    print("Saving Data")
    #COMMENT OUT storage options when in production TODO
    # Fulldf.to_csv(filename_User, storage_options={"token": credentials})
    Fulldf.to_csv(filename_User)