import pandas as pd
import numpy as np

from pythainlp import word_tokenize
import nltk
from nltk.corpus import stopwords
from pythainlp.corpus.common import thai_stopwords
from pythainlp.corpus.common import thai_words

import pickle
import joblib
import re
import string

from helper import *

from sklearn.feature_extraction.text import TfidfVectorizer 
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from bertopic.vectorizers import ClassTfidfTransformer

from umap import UMAP
from bertopic import BERTopic
from transformers.pipelines import pipeline

from google.cloud import firestore
from google.cloud import storage
from google.cloud import logging
import logging as log

import glob
import os
import io
from io import StringIO
from io import BytesIO

import json
import ast
from pandas.io.json import json_normalize
from urllib.parse import unquote
from datetime import datetime
from datetime import date

def write_to_blob(bucket_name,file_name,content):
    
    #Use the top one for production TODO
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    #Use this one for different gcp environment
    # credentials = "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    # client = storage.Client(project='connect-x-production').from_service_account_json(credentials)
    # bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_name)
    blob.upload_from_string(content)
    
def read_from_blob(bucket_name,file_name):
    
    #Use the top one for production TODO
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    #Use this one for different gcp environment
    # credentials = "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    # client = storage.Client(project='connect-x-production').from_service_account_json(credentials)
    # bucket = client.get_bucket(bucket_name)
    
    blob = bucket.blob(file_name)
    
    return blob.download_as_string().decode()

def MapToUser():
    #Logging into client Bucket
    credentials = "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    
    # TODO Uncomment for production
    client = storage.Client()
    # client = storage.Client(project='connect-x-production').from_service_account_json(credentials)
    logging_client = logging.Client()
    logging_client.setup_logging()

    BUCKET_NAME = 'connect-x-production.appspot.com'
    filepath = "gs://connect-x-production.appspot.com"
    bucket = client.get_bucket(BUCKET_NAME)

    #Getting the inputs into the Bertopic Model
    #COMMENT OUT storage options when in production TODO
    print("Getting Input Text of model")
    # input_text = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/InputText/input_text.csv",
    #                          storage_options={"token": credentials})
    input_text = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/InputText/input_text.csv")

    #Filenames for all models
    vec_filename = "Organizes/pJoo5lLhhAbbofIfYdLz/AI/SegmentationModels/vectorizer_model.pickle"
    bert_filename = "Organizes/pJoo5lLhhAbbofIfYdLz/AI/SegmentationModels/bertopic_model.pkl"

    #Getting CountVectorizer model
    print("Getting CountVectorizer model")
    blobCV = bucket.blob(vec_filename)
    pickleCV = blobCV.download_as_bytes()
    cv_model = pickle.loads(pickleCV)

    #Getting Bertopic Model
    print("Getting Bertopic model")
    blobBERTopic = bucket.blob(bert_filename)
    model_file = BytesIO()
    blobBERTopic.download_to_file(model_file)
    topic_model = joblib.load(model_file)

    #Getting mapped links and topics
    print("Getting Mapped Topics")
    
    #COMMENT OUT storage options when in production TODO
    # df_mapped_Topic = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/Topics/Topics.csv",
    #                               storage_options={"token": credentials})
    df_mapped_Topic = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/Topics/Topics.csv")

    #Getting cleaned user data
    print("Getting cleaned user data")
    #TODO Uncomment for production
    client = storage.Client()
    # client = storage.Client(project='connect-x-production').from_service_account_json(
    #         "/home/jupyter/krungsri/connect-x-production-firebase-adminsdk-gg382-b1563f2b74.json"
    #     )
    logging_client = logging.Client()
    logging_client.setup_logging()
    BUCKET_NAME = 'connect-x-production.appspot.com'
    bucket = client.get_bucket(BUCKET_NAME)
    userblobs = bucket.list_blobs(prefix = "Organizes/pJoo5lLhhAbbofIfYdLz/AI/CleanedUserData")
    userFiles = [blob for blob in userblobs if 'csv' in blob.name]

    li_reLinks = []
    userPath = "gs://connect-x-production.appspot.com"
    for file in userFiles:
        n_path = os.path.join(userPath, file.name)
        li_reLinks.append(n_path)

    li_reLinks = sorted(li_reLinks, reverse=True)
    
    #COMMENT OUT storage options when in production TODO
    # df_users_cleaned = pd.read_csv(li_reLinks[0], index_col='createdDate',
    #                               storage_options={"token": credentials})
    df_users_cleaned = pd.read_csv(li_reLinks[0], index_col='createdDate')

    #Mapping Topics to Users
    print("Mapping Topics to Users")
    corresp = dict()
    for key, val in zip(df_mapped_Topic.url,df_mapped_Topic.Topic):
        corresp[key] = val

    #_____________________________________________________________________________________________________________
    df_users_cleaned['Topic'] = df_users_cleaned.loc[:,'cx_link'].map(corresp) 
    
    print("Cleaning Up")
    df_users_cleaned = df_users_cleaned[list(map(includePPTCP,tqdm(df_users_cleaned.cx_link.to_list())))]
    df_users_cleaned = df_users_cleaned[list(map(removeWealth,tqdm(df_users_cleaned.cx_link.to_list())))]
    df_users_cleaned = df_users_cleaned[list(map(removeBusiness,tqdm(df_users_cleaned.cx_link.to_list())))]
    df_users_cleaned = df_users_cleaned[list(map(removeBank,tqdm(df_users_cleaned.cx_link.to_list())))]
    df_users_cleaned = df_users_cleaned[list(map(removeJP,tqdm(df_users_cleaned.cx_link.to_list())))]

    df_users_cleaned = df_users_cleaned.sort_index()
    
    today = date.today()
    segPath = "gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/UserSegments"
    filename_Seg = segPath+"/UserSegments_"+today.strftime("%Y%m%d")+".csv"
    
    print("Saving Files")
    last_date = 'Organizes/pJoo5lLhhAbbofIfYdLz/AI/LastDate.txt'
    write_to_blob(BUCKET_NAME,last_date,df_users_cleaned.index[-1])
    
    #COMMENT OUT storage options when in production TODO
    # df_users_cleaned.to_csv(filename_Seg, storage_options={"token": credentials})
    df_users_cleaned.to_csv(filename_User)

