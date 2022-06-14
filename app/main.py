from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
# import pythainlp
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.feature_extraction.text import TfidfTransformer
from urllib.parse import quote, unquote
import ast
from google.cloud import firestore

cossim_matrix = pd.read_csv('data/cossim_matrix.csv')
recommended = pd.read_csv('data/recommended.csv')

class Item(BaseModel):
    url: str
    cx_cookie: str

app = FastAPI()

@app.post("/get_recommend/")
async def create_item(item: Item):
    input_url = item.url
    input_url = unquote(input_url)
    cx_cookie = item.cx_cookie
    db = firestore.Client()
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/activities/data')
    query = doc_ref.where(u'cx_cookie', u'==', cx_cookie).get()
    history = [q.to_dict()['cx_web_url_fullpath'].split('?')[0] for q in query]
    history = list(set(history))
    
    url = '/'.join(input_url.split('/')[4:])
    description = recommended.loc[recommended['DocumentUrlPath'].str.contains(url)]
    cosine_score = cossim_matrix.values.tolist()
    print(type(cosine_score))
    
    result = {}
    count = 0
    list_recommend = ast.literal_eval(description['recommend'].values[0])
    for idx in list_recommend:
        score = cosine_score[description.index[0]][idx]
        text = recommended.iloc[recommended.index == idx]['DocumentUrlPath'].values[0]
        if any(text not in his for his in history):
            result[count] = {'url': text,
                         'similar_score': 1 - score}
            count += 1
        if count == 3:
            break
    return JSONResponse(content=result)

@app.get("/")
async def test():
    return "Hello Mintel"
