import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import pickle

from fastapi import FastAPI, Response
from fastapi.responses import FileResponse
from fastapi.responses import HTMLResponse

from typing import Optional
from pydantic import BaseModel

from google.cloud import firestore
from utils import check_path, check_path2, check_score, update_tag_unknown

class Item(BaseModel):
    input_file_gs: str
    
cata = ['deposit','loans','card','bancassurance','mutual-fund','banking-services','digital-banking']


app = FastAPI()


@app.post("/segmentation")
def process(item: Item):  
    
    df_file = pd.read_csv(item.input_file_gs, index_col=0)

    df_file = df_file[['cx_cookie','cx_web_url_fullpath','mainPage']]

    # ------------- check url (product or not) -------------
    df_file['score'] = df_file.apply(lambda x: check_score(x['mainPage'],x['cx_web_url_fullpath']),axis=1)
    df_file = df_file[df_file['score'] != -1]
    df_file.reset_index(drop=True)

    df_file.drop('mainPage',axis=1,inplace=True)
    # print(df_file)

    df_file['link'] = df_file.cx_web_url_fullpath.apply(check_path)
    df_file = df_file[df_file.link == True]

    df_file['catalogue'] = df_file.cx_web_url_fullpath.apply(check_path2)
    df_file.drop(df_file[df_file.catalogue == False].index, inplace=True)

    df_file.drop(['cx_web_url_fullpath','link'],axis=1,inplace=True)

    # ------------- join score and cat -------------
    tmp = df_file.groupby(['cx_cookie', 'catalogue']).max().reset_index()

    join_df_file = pd.DataFrame(columns=cata)
    join_df_file['cx_cookie'] = tmp.cx_cookie.unique()
    join_df_file.set_index('cx_cookie',inplace=True)
    
    for index,row in tmp.iterrows():
        join_df_file.loc[row.cx_cookie,row.catalogue] = row.score

    join_df_file.fillna(0,inplace=True)

    # ------------- K-means -------------
    tmp_df = join_df_file.copy()
    
    filename = 'models/std_scale.pb'
    scaler = pickle.load(open(filename, 'rb'))
    array_normalized = scaler.fit_transform(join_df_file)
    df_normalized = pd.DataFrame(array_normalized)
    df_normalized.head()

    filename = 'models/kmeans-7.pb'
    kmeans = pickle.load(open(filename, 'rb'))
    cluster_predictions = kmeans.predict(df_normalized)
    tmp_df['Group'] = cluster_predictions

    cluster_name = {}

    for i in range(0,7):
        max_mean = -1
        max_mean_name = 'deposit'
        for j,k in zip( tmp_df[tmp_df.Group==i].mean(), range(0,7) ):
            if j > max_mean:
                max_mean =j
                max_mean_name = cata[k]
        cluster_name[i] = max_mean_name

    tmp_df.Group.replace(cluster_name,inplace=True)
    
    db = firestore.Client()
    for row in tmp_df.itertuples():
        update_tag_unknown(db, row.Index, row.Group, cata)
    
    return 'success'




@app.get("/")
def root():
    return {"Hello": "World"}

#json