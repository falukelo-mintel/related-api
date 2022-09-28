from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
import re
from google.cloud import firestore
import json
import os
import requests
from google.cloud import logging
import logging as log

from utils import train_recommendation


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.post("/train_related/")
# async def train_related(item: Item_rel):
#     client = storage.Client()
#     logging_client = logging.Client()
#     logging_client.setup_logging()
#     # BUCKET_NAME = os.environ['BUCKET_NAME']
#     BUCKET_NAME = item.bucket_name
#     bucket = client.get_bucket(BUCKET_NAME)
#     blobs = bucket.list_blobs(prefix='Organizes/pJoo5lLhhAbbofIfYdLz/AI/data/')
#     all_files = [f'gs://{BUCKET_NAME}/{blob.name}' for blob in blobs if 'csv' in blob.name]
#     # print(all_files)
#     li_product = []
#     li_article = []
#     for filename in all_files:
#         df = pd.read_csv(filename, index_col=None, header=0, usecols=['cx_web_url_fullpath', 'cx_cookie', 'cx_event'])
#         li_product.append(df.loc[df["cx_web_url_fullpath"].str.contains("/personal/", na=False)])
#         li_article.append(df.loc[df["cx_web_url_fullpath"].str.contains("plearn-plearn|krungsri-the-coach", na=False)])
#         del df
#     df_article = pd.concat(li_article, axis=0, ignore_index=True)
#     df_product = pd.concat(li_product, axis=0, ignore_index=True)
#     log.info('Read files from GCS already.')
#     del li_product
#     del li_article
#     # df_article = df.loc[df["cx_web_url_fullpath"].str.contains("plearn-plearn|krungsri-the-coach", na=False)]
#     # df_product = df.loc[df["cx_web_url_fullpath"].str.contains("/personal/", na=False)]
#     unique_cookies = list(set(df_article['cx_cookie'].values.tolist()))
#     df_tmp = pd.DataFrame(columns=df_product.columns)
#     df_tmp = pd.concat([df_tmp, df_product.loc[df_product["cx_cookie"].isin(unique_cookies)]], ignore_index=True)
#     df_tmp = pd.concat([df_tmp, df_article.loc[df_article["cx_cookie"].isin(unique_cookies)]], ignore_index=True)
#     cookie_dup = list(set(df_tmp['cx_cookie'].values.tolist()))
#     df_tmp['cx_web_url_fullpath'] = df_tmp['cx_web_url_fullpath'].str.split('?', 1).str[0]
#     df_tmp = df_tmp.loc[df_tmp['cx_event'] == 'Pageview']
#     unique_cookies = list(set(df_tmp['cx_cookie'].values.tolist()))
#     unique_url = list(set(df_tmp['cx_web_url_fullpath'].values.tolist()))
#     df = pd.DataFrame()
#     data = {}
#     for cookie in unique_cookies:
#         data[cookie] = {'products': {},
#                         'articles': {}}
#     for row in df_tmp.iterrows():
#         cookie = row[1]['cx_cookie']
#         if row[1]['cx_web_url_fullpath'] in unique_url:
#             try:
#                 if '/personal' in row[1]['cx_web_url_fullpath']:
#                     data[cookie]['products'][row[1]['cx_web_url_fullpath']] += 1
#                 elif '/plearn-plearn' in row[1]['cx_web_url_fullpath'] or '/krungsri-the-coach' in row[1]['cx_web_url_fullpath']:
#                     data[cookie]['articles'][row[1]['cx_web_url_fullpath']] += 1
#             except KeyError:
#                 if '/personal' in row[1]['cx_web_url_fullpath']:
#                     data[cookie]['products'][row[1]['cx_web_url_fullpath']] = 1
#                 elif '/plearn-plearn' in row[1]['cx_web_url_fullpath'] or '/krungsri-the-coach' in row[1]['cx_web_url_fullpath']:
#                     data[cookie]['articles'][row[1]['cx_web_url_fullpath']] = 1
#     data_gen = []
#     for d in data:
#         articles = data[d]['articles']
#         products = data[d]['products']
#         if products == {} or articles =={}:
#             continue
#         for idx_a, a in enumerate(articles):
#             for p in products:
#                 tmp_lst = [','.join(list(articles.keys())[:idx_a+1]), p]
#             data_gen.append(tmp_lst)
#     df_associations = pd.DataFrame(data_gen, columns=['antecedents', 'consequents'])
#     df_associations = df_associations.groupby(['antecedents','consequents']).size().reset_index().rename(columns={0:'frequency'})
#     df_associations['frequency_antecedent'] = df_associations.groupby('antecedents')['antecedents'].transform('count')
#     n = len(df_associations)
#     df_associations['support'] = df_associations['frequency']/n
#     df_associations['confidence'] = df_associations['frequency']/df_associations['frequency_antecedent']
#     df_associations['antecedents'] = df_associations['antecedents'].str.replace('https://www.krungsri.com/bank', 'https://www.krungsri.com')
#     df_associations['antecedents'] = df_associations['antecedents'].str.replace('.html', '')
#     bucket = client.get_bucket(BUCKET_NAME)
#     blob_source_name = 'Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/rel/main/data/df_associations.csv'
#     source_blob = bucket.blob(blob_source_name)
#     if source_blob.exists():
#         blob_dest_name = f'Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/rel/tmp/data/df_associations-{source_blob.time_created}.csv'

#         new_blob = bucket.copy_blob(
#             source_blob, bucket, blob_dest_name)
#         source_blob.delete()
#         log.info('Move data model to /tmp/')
#     else:
#         log.info(f'Source blob [{blob_source_name}] not exists.')
#     df_associations.to_csv(f'gs://{BUCKET_NAME}/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/rel/main/data/df_associations.csv', index=None)
#     log.info('Success.')
#     return 'success'

@app.get("/train_recommendation/")
async def train_recommendation():
    try:
        db = firestore.Client()
        doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
        arts = list(doc_ref.stream())
        arts_dict = list(map(lambda x: x.to_dict(), arts))

        #### Plean ####
        df = pd.DataFrame(arts_dict)
        train_recommendation(df, 'recommended')

        #### The coach #### recommended_coach
        df_coach = df.loc[df.link.str.contains('/krungsri-the-coach/')]
        df_coach = df_coach.reset_index(drop = True)
        train_recommendation(df_coach, 'recommended_coach')
        
        response = requests.get("https://recommend-api-0742218-vj5uu3gpya-as.a.run.app/update")
        log.info(response)
        return JSONResponse({'status': 200})
    except:
        return JSONResponse({'status': 400})

@app.get("/")
async def test():
    return "Hello Mintel Training AI"
