from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
import json
import os
from google.cloud import storage
import subprocess
import pythainlp
import re
from sklearn.feature_extraction.text import TfidfTransformer
from utils import cosine_related_similarity, deepcut

app = FastAPI()
class Item_rel(BaseModel):
    bucket_name: str

class Item_rec(BaseModel):
    input_file_gs: str
    num_recommend: int

@app.post("/train_related/")
async def train_related(item: Item_rel):
    client = storage.Client()
    # BUCKET_NAME = os.environ['BUCKET_NAME']
    BUCKET_NAME = item.bucket_name
    bucket = client.get_bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix='krungsri/')
    all_files = [f'gs://{BUCKET_NAME}/{blob.name}' for blob in blobs]
    # print(all_files)
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    df = pd.concat(li, axis=0, ignore_index=True)
    del li
    df_article = df.loc[df["cx_web_url_fullpath"].str.contains("plearn-plearn|krungsri-the-coach", na=False)]
    df_product = df.loc[df["cx_web_url_fullpath"].str.contains("/personal/", na=False)]
    unique_cookies = list(set(df_article['cx_cookie'].values.tolist()))
    df_tmp = pd.DataFrame(columns=df_product.columns)
    count = 0
    for cookie in unique_cookies:
        if cookie in df_product["cx_cookie"].values.tolist():
            count += 1
            df_tmp = pd.concat([df_tmp, df_product.loc[df_product["cx_cookie"] == cookie]], ignore_index=True)
            df_tmp = pd.concat([df_tmp, df_article.loc[df_article["cx_cookie"] == cookie]], ignore_index=True)
    cookie_dup = list(set(df_tmp['cx_cookie'].values.tolist()))
    df_tmp['cx_web_url_fullpath'] = df_tmp['cx_web_url_fullpath'].str.split('?', 1).str[0]
    df_tmp = df_tmp.loc[df_tmp['cx_event'] == 'Pageview']
    unique_cookies = list(set(df_tmp['cx_cookie'].values.tolist()))
    unique_url = list(set(df_tmp['cx_web_url_fullpath'].values.tolist()))
    df = pd.DataFrame()
    data = {}
    for cookie in unique_cookies:
        df_temp = df_tmp.loc[df_tmp["cx_cookie"] == cookie]
        data[cookie] = {'products': {},
                        'articles': {}}
        for row in df_temp.iterrows():
            if row[1]['cx_web_url_fullpath'] in unique_url:
                try:
                    if '/personal' in row[1]['cx_web_url_fullpath']:
                        data[cookie]['products'][row[1]['cx_web_url_fullpath']] += 1
                    elif '/plearn-plearn' in row[1]['cx_web_url_fullpath'] or '/krungsri-the-coach' in row[1]['cx_web_url_fullpath']:
                        data[cookie]['articles'][row[1]['cx_web_url_fullpath']] += 1
                except KeyError:
                    if '/personal' in row[1]['cx_web_url_fullpath']:
                        data[cookie]['products'][row[1]['cx_web_url_fullpath']] = 1
                    elif '/plearn-plearn' in row[1]['cx_web_url_fullpath'] or '/krungsri-the-coach' in row[1]['cx_web_url_fullpath']:
                        data[cookie]['articles'][row[1]['cx_web_url_fullpath']] = 1
    data_gen = []
    for d in data:
        articles = data[d]['articles']
        products = data[d]['products']
        for idx_a, a in enumerate(articles):
            for p in products:
                tmp_lst = [','.join(list(articles.keys())[:idx_a+1]), p]
            data_gen.append(tmp_lst)
    df_associations = pd.DataFrame(data_gen, columns=['antecedents', 'consequents'])
    df_associations = df_associations.groupby(['antecedents','consequents']).size().reset_index().rename(columns={0:'frequency'})
    df_associations['frequency_antecedent'] = df_associations.groupby('antecedents')['antecedents'].transform('count')
    n = len(df_associations)
    df_associations['support'] = df_associations['frequency']/n
    df_associations['confidence'] = df_associations['frequency']/df_associations['frequency_antecedent']
    df_associations['antecedents'] = df_associations['antecedents'].str.replace('https://www.krungsri.com/bank', 'https://www.krungsri.com')
    df_associations['antecedents'] = df_associations['antecedents'].str.replace('.html', '')
    df_associations.to_csv('data/df_associations.csv', index=None)
    subprocess.call(['sh', './gitprocess_rel.sh'])
    return 'success'

@app.post("/train_recommendation/")
async def train_recommendation(item: Item_rec):
    df = pd.read_csv(item.input_file_gs)
    df_text = df[['NodeName', 'DocumentUrlPath', 'NodeAliasPath', 'DocumentPageTitle','DocumentPageDescription']]
    df_text = df_text.loc[(df_text['DocumentPageTitle'].notna() & df_text['DocumentPageDescription'].notna())]
    df_text.reset_index(inplace = True,drop = True)
    df_text['DocumentPageDescription'] = df_text['DocumentPageDescription'].apply(lambda x:re.sub('[^A-Za-z0-9ก-๙ ]+', '', x))
    df_text['DocumentPageTitle'] = df_text['DocumentPageTitle'].apply(lambda x:re.sub('[^A-Za-z0-9ก-๙ ]+', '', x))
    cVec = deepcut(df_text)
    
    tf_trans = TfidfTransformer()
    tfIdf_matrix = tf_trans.fit_transform(cVec.values)
    tfidf_df = pd.DataFrame(tfIdf_matrix.toarray(), columns = cVec.columns)
    cosine_matrix = cosine_related_similarity(tfidf_df, item.num_recommend)
    
    df_text['recommend'] = cosine_matrix['TopThree']
    df_text['DocumentUrlPath'] = df_text['DocumentUrlPath'].fillna(df_text['NodeAliasPath'])

    df_text.to_csv('data/recommended.csv', index=None)
    return 'success'

@app.get("/")
async def test():
    return "Hello Mintel Training AI"
