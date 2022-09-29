from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
from urllib.parse import quote, unquote
import ast
from google.cloud import firestore
from fastapi.middleware.cors import CORSMiddleware

recommended_all = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recommended.csv')
recommended_coach = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recommended_coach.csv')

db = firestore.Client()
doc_arts = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
arts = list(doc_arts.order_by(u'lastModified', direction=firestore.Query.DESCENDING).stream())
arts_dict = list(map(lambda x: x.to_dict(), arts))
df_arts = pd.DataFrame(arts_dict)

# class Item(BaseModel):
#     url: str
#     cx_cookie: str

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# @app.get("/get_recommend/")
# async def create_item(url: str, cookie: str):
#     input_url = url
#     input_url = unquote(input_url)
#     cx_cookie = cookie
#     db = firestore.Client()
#     doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/activities/data')
#     # query = doc_ref.where(u'cx_cookie', u'==', cx_cookie).order_by(u'createdDate').limit(300).get()
#     query = doc_ref.where(u'cx_cookie', u'==', cx_cookie).order_by(u'createdDate').limit(300).get()
#     history = [q.to_dict()['cx_web_url_fullpath'].split('?')[0] for q in query]
#     history = list(set(h for h in history if '/krungsri-the-coach/' in h or '/plearn-plearn/' in h))
    
#     content_name = input_url.split('/')[-1]
#     description = recommended.loc[recommended["link"].str.contains(content_name, na=False)]
    
#     result = {"related_article": []}
#     list_recommend = ast.literal_eval(description['recommend'].values[0])
#     cont_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
#     urls = []
#     for idx in list_recommend:
#         text = recommended.iloc[recommended.index == int(idx)]['link'].values[0]
#         if '/krungsri-the-coach/' in input_url:
#             if '/plearn-plearn/' in text:
#                 continue
#         text2 = quote(text.split('/')[-1])
#         if all(text2 not in his for his in history):
#             urls.append(text)
#         if urls.__len__() == 3:
#             break
#     try:
#         query = cont_ref.where(u'link', u'in', urls).get()
#         for qry in query:
#             q = qry.to_dict()
#             del q['textContent']
#             del q['createdBy']
#             del q['lastModified']
#             del q['createdDate']
#             del q['modifiedBy']
#             result['related_article'].append(q)
#     except Exception as e:
#         print(e)
#     if len(result['related_article']) < 3:
#         while True:
#             lim = 3 - len(result['related_article'])
#             query = cont_ref.order_by(u'createdDate').limit(lim).get()
#             for qry in query:
#                 q = qry.to_dict()
#                 del q['textContent']
#                 del q['createdBy']
#                 del q['lastModified']
#                 del q['createdDate']
#                 del q['modifiedBy']
#                 text2 = quote(q['link'].split('/')[-1])
#                 if all(text2 not in his for his in history):
#                     result['related_article'].append(q)
#             if len(result['related_article']) >= 3:
#                 break
    
#     return JSONResponse(content=result)

@app.get("/get_recommend/")
async def get_recommend(url: str, cookie: str):
    input_url = url
    input_url = unquote(input_url)
    cx_cookie = cookie
    db = firestore.Client()
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/activities/data')
    
    query = doc_ref.where(u'cx_cookie', u'==', cx_cookie).order_by(u'lastModified', direction=firestore.Query.DESCENDING).limit(500).get()
    history = [q.to_dict()['cx_web_url_fullpath'].split('?')[0] for q in query]
    history = list(set(h for h in history if '/krungsri-the-coach/' in h or '/plearn-plearn/' in h))
    content_name = input_url.split('/')[-1]
    
    if '/krungsri-the-coach/' in input_url:
        recommended = recommended_coach.copy()
        df_articles = df_arts.loc[df_arts.link.str.contains('/krungsri-the-coach/')]
    elif '/plearn-plearn/' in input_url:
        recommended = recommended_all.copy()
        df_articles = df_arts.copy()
        
    description = recommended.loc[recommended["link"].str.contains(content_name, na=False)]
    list_recommend = ast.literal_eval(description['recommend'].values[0])
    
    result = {"related_article": []}
    urls = []
    for idx in list_recommend:
        text = recommended.iloc[recommended.index == int(idx)]['link'].values[0]
        text2 = quote(text.split('/')[-1])
        if all(text2 not in his for his in history):
            urls.append(text)
        if urls.__len__() == 3:
            break
    if len(urls) < 3:
        for text in df_articles.link.values:
            text2 = quote(text.split('/')[-1])
            if all(text2 not in his for his in history):
                urls.append(text)
            if urls.__len__() == 3:
                break
    for url in urls:
        text = url.split('/')
        query = df_articles.loc[df_articles.link.str.contains(text[4]) & df_articles.link.str.contains(text[-1])]
        query = query.reset_index(drop = True)
        del query['textContent']
        del query['createdBy']
        del query['lastModified']
        del query['createdDate']
        del query['modifiedBy']
        query = query.to_dict('index')
        result['related_article'].append(query[0])
        
    return JSONResponse(content=result)

@app.get("/")
async def test():
    return "Recommendation Article by Mintel."

@app.get("/update")
async def update():
    global recommended_all
    global recommended_coach
    global df_arts
    global df_arts
    recommended_all = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recommended.csv')
    recommended_coach = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recommended_coach.csv')
    db = firestore.Client()
    doc_arts = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
    arts = list(doc_arts.order_by(u'lastModified', direction=firestore.Query.DESCENDING).stream())
    arts_dict = list(map(lambda x: x.to_dict(), arts))
    df_arts = pd.DataFrame(arts_dict)
    return JSONResponse({"status": 200})