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
import logging as log
import requests
from datetime import datetime
from datetime import date
import re
import string

import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.multiclass import OneVsRestClassifier
from sklearn.model_selection import train_test_split
from sklearn.svm import LinearSVC
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_recall_fscore_support, accuracy_score
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import MinMaxScaler

from pythainlp import word_tokenize
from pythainlp.corpus.common import thai_stopwords
import re
from pythainlp.util import isthai
from pythainlp.corpus.common import thai_words
from pythainlp.util import Trie

import matplotlib.pyplot as plt


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
    if(isinstance(val,str)):
        return val
    
    if ck in df:
        return ck
    return ck

def unquoteLink(link):
    link = unquote(link)
    link = re.sub(" ","",link)
    return link

def personal_cut(word):
    if word.find('/th/personal')>0:
        word = re.sub('/th/','/',word)
        word.replace(" ","")
    return word

def reconstructLink(link):
    
    if not isinstance(link, str):
        return link
    #replace double slash after .com
    K = '/'
    link = link.replace('//', K).replace(K, '//', 1)
    
    #take away ad
    link = re.sub('\?.*$','',link)
    link = re.sub('\#.*$','',link)
    #personal
    if link.find('com/personal')>0:
        link = re.sub('com/personal','com/th/personal',link)
    if link.find('/en/personal')>0:
        link = re.sub('/en/personal','/th/personal',link)
    #plearn plearn
    if link.find('com/plearn-plearn')>0:
        link = re.sub('com/plearn-plearn','com/th/plearn-plearn',link)
    if link.find('/en/plearn-plearn')>0:
        link = re.sub('/en/plearn-plearn','/th/plearn-plearn',link)
    # if link.find('plearn-plearn//plearn-plearn/')>0:
    #     link = re.sub('plearn-plearn//plearn-plearn/','plearn-plearn/',link)
    #The coach
    if link.find('com/krungsri-the-coach')>0:
        link = re.sub('com/krungsri-the-coach','com/th/krungsri-the-coach',link)
    if link.find('/en/krungsri-the-coach')>0:
        link = re.sub('/en/krungsri-the-coach','/th/krungsri-the-coach',link)
    
    #Unquote links
    link = unquoteLink(link)
    return link

def includePPTCP(link):
    if link.find('krungsri-the-coach')>=0 or link.find('plearn-plearn')>=0 or link.find('/personal/')>=0:
        return True
    return False

def removeWealth(link):
    if link.find('.com/wealth')>=0 or link.find('.com/th/wealth')>=0 or link.find('.com/en/wealth')>=0:
        return False
    return True

def removeBusiness(link):
    if link.find('.com/business')>=0 or link.find('.com/th/business')>=0 or link.find('.com/en/business')>=0:
        return False
    return True

def removeBank(link):
    if link.find('.com/bank/')>=0:
        return False
    return True

def removeJP(link):
    if link.find('/jp/')>=0:
        return False
    return True
#----

def validateLink(link):
    try:
    #Get Url
        r = requests.get(link, allow_redirects=True)
        # if the request succeeds 
        if r.status_code == 200:
            if r.url.find("404")>=0:
                return ""
        else:
            return ""

    #Exception
    except requests.exceptions.RequestException as e:
        return ""
    
    return r.url.strip('/')

def preprocess_text(text):
    text = text.replace('ํา','ำ')
    text = text.replace('เเ','แ')
    text = re.sub('http://\S+|https://\S+', '', text)
    text = re.sub('[^A-Za-zก-์\s]+|ๆ', '', text)
    return text

def clean_msg(msg):
    
    msg = msg.replace('ํา','ำ')
    msg = msg.replace('เเ','แ')
    msg = re.sub('http://\S+|https://\S+', '', msg)
    # ลบ text ที่อยู่ในวงเล็บ <> ทั้งหมด
    msg = re.sub(r'<.*?>','', msg)
    
    # ลบ hashtag
    msg = re.sub(r'#','',msg)
    
    # ลบ เครื่องหมายคำพูด (punctuation)
    for c in string.punctuation:
        msg = re.sub(r'\{}'.format(c),'',msg)
    
    # ลบ separator เช่น \n \t
    msg = ' '.join(msg.split())
    msg = re.sub('[^A-Za-zก-์\s]+|ๆ', '', msg)
    
    return msg.strip()


stop_words = [t for t in list(thai_stopwords())]

def text_tokenizer(text):
    terms = [k.strip() for k in word_tokenize(text,keep_whitespace=False, engine='nercut') if len(k.strip()) > 0 and k.strip() not in stop_words]

    return [t for t in terms if len(t) > 1 or t is not None]