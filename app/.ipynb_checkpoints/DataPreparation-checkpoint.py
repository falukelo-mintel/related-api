# From custom modules
from TFIDF import *

# Import common modules
from kmodes.kprototypes import KPrototypes
import pandas as pd
import numpy as np
import glob
import os

import json
import ast
from pandas.io.json import json_normalize
from urllib.parse import unquote
import regex as re

from google.cloud import firestore
from google.cloud import storage
from google.cloud import logging
import logging as log


def only_dict(d):
    return ast.literal_eval(d)

def list_of_dicts(ld):
    return dict([(list(d.values())[1], list(d.values())[0]) for d in ast.literal_eval(ld)])

def split_web(string):
    if(string != string):
        return np.nan
    a = string.split('/')
    return a[2]

def unwantedWeb(link):
    if(split_web(link) == "stage10th.com"):
        return ""
    return link

def checkUnknown(ck,val,df):
    ck = ck.split('|')[1]
    if(isinstance(val,str)):
        return val
    
    if ck in df:
        return ck
    return ck

def reconstructLink(link):
    link = unquote(link)
    link = link.replace(" ","")
    link = re.sub('\?.*$','',link)
    link = re.sub('\#.*$','',link)
    if link.find('/th/personal')>0:
        link = re.sub('/th/','/',link)
    
    return link

def CleanUserData():
    #-----------------------------------------------------------------------------------------------------------------------
    # For 2 things:
    # 1. Cleaning User data
    # 2. Mapping product categories to users data
    #-----------------------------------------------------------------------------------------------------------------------
    # path = 'data/' # use your path
    
    client = storage.Client()
    logging_client = logging.Client()
    logging_client.setup_logging()
    
    BUCKET_NAME = 'connect-x-production.appspot.com'
    bucket = client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix='Organizes/pJoo5lLhhAbbofIfYdLz/AI/data')
    
    all_files = [f'gs://{BUCKET_NAME}/{blob.name}' for blob in blobs if 'csv' in blob.name]

    li = []
    columns = ["unknownContact","cx_cookie",
               "cx_fingerprint","cx_web_url_fullpath","cx_event"]
    
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        df = df[columns]
        df = df[df["cx_event"] == "Pageview"]
        df.insert(0,"fingerprint + cookie",df['cx_fingerprint'].astype(str) +"|"+ df["cx_cookie"])
        df.drop(["cx_fingerprint","cx_cookie"], axis=1,inplace=True)
        df["cx_web_url_fullpath"].apply(lambda x: unwantedWeb(x))
        df = df[df["cx_web_url_fullpath"] != ""]
        A = json_normalize(df['unknownContact'].apply(only_dict).tolist()).add_prefix('unknownContact.')
        df[['unknownContact.value', 'unknownContact.label']] = A
        del A
        df.drop("unknownContact", axis = 1, inplace = True)

        #using unknown label
        df['unknownContact.label'] = df.apply(lambda x: checkUnknown(x['fingerprint + cookie'],x['unknownContact.label'],df['unknownContact.label'].to_numpy()), axis=1).values

        li.append(df)
        del df
        
    frame = pd.concat(li, axis=0, ignore_index=True)
    
    del li
    
    Fulldf = frame[frame['cx_web_url_fullpath'].notna()]
    Fulldf['cx_web_url_fullpath'] = list(map(reconstructLink, frame['cx_web_url_fullpath'].to_list()))
    
    all_prods = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/state/product_segments.csv")
    corresp2 = dict()
    for key, val in zip(all_prods.cx_link, clusters_prod):
        corresp2[key] = val 
    
    Fulldf['cat_product'] = Fulldf.loc[:,'cx_web_url_fullpath'].map(corresp2)
    df_temp = Fulldf[Fulldf.cat_product.isnull() == False]
    
    df_cleaned = df_temp[['fingerprint + cookie','unknownContact.label','unknownContact.value','cx_web_url_fullpath','cat_product']]
    df_cleaned.to_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/state/mappedUsers.csv", index= False)
    
    #-----------------------------------------------------------------------------------------------------------------------


def ProductClustering():
    #-----------------------------------------------------------------------------------------------------------------------
    # For Clustering products together
    # run each time there are new products
    #-----------------------------------------------------------------------------------------------------------------------
    #Read products for first training
    all_prods = get_all_prods()
    
    prods_matrix = tfidf_vectorize(all_prods)
    prods_matrix["product"] = all_prods.Type
    
    #Fitting into KPrototypes
    n_clusters = 6
    kmp = KPrototypes(n_clusters=n_clusters, init='Cao', random_state=42, verbose = 0)
    global clusters_prod 
    clusters_prod = kmp.fit_predict(prods_matrix, categorical=[500])
    
    corresp = dict()
    for key, val in zip(all_prods.textContent, clusters_prod):
        corresp[key] = val 
        
    all_prods['categ_product'] = all_prods.loc[:, 'textContent'].map(corresp)
    all_prods.to_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/state/product_segments.csv", index= False)   
    
    #-----------------------------------------------------------------------------------------------------------------------

    
def get_all_prods():
    db = firestore.Client()
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/krungsriProduct/data')
    products = list(doc_ref.stream())
    products_dict = list(map(lambda x: x.to_dict(), products))
    df_prods = pd.DataFrame(products_dict)
    df_prods = df_prods.loc[df_prods.visible != False]
    df_prods = df_prods.reset_index(drop=True)
    
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
    articles = list(doc_ref.stream())
    articles_dict = list(map(lambda x: x.to_dict(), articles))
    df_atrs = pd.DataFrame(articles_dict)
    
    all_prods = pd.DataFrame()
    all_prods[['link', 'cx_Name', 'textContent']] = df_prods[['url', 'cx_Name', 'description']]
    all_prods = pd.concat([all_prods, df_atrs[['link', 'cx_Name', 'textContent']]])
    all_prods = all_prods.reset_index(drop=True)
    all_prods['Type'] = list(map(map_type,  all_prods['link'].values.tolist()))
    all_prods.columns = ['cx_link', 'cx_Name', 'textContent', 'Type']
    
    return all_prods

def map_type(item):
    item = item.replace('/th/', '/')
    res = item.split('/')[3]
    if 'coach' in res:
        res = 'coach'
    elif 'business' in res:
        res = 'personal'
    return res