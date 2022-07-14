from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
from utils import related_similarity_content_tfidf
from google.cloud import firestore
data_model = pd.read_csv('data/df_associations.csv')
df_recomendation = pd.read_csv('data/recommended-20.csv')

class Item(BaseModel):
    url: str
    cookie: str

app = FastAPI()

@app.post("/get_related/")
async def get_related(item: Item):
    db = firestore.Client()
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/krungsriProduct/data')
    input_url = item.url
    df_query = data_model.loc[data_model['antecedents'] == input_url]
    df_final = df_query.sort_values('confidence', ascending=False)
    results_product = []
    consequents = df_final['consequents'].values.tolist()
    if len(consequents) == 0:
        lst = related_similarity_content_tfidf(data_model, input_url, df_recomendation)
        lst_sim = [r for r in lst if r not in consequents]
        results_product = consequents + lst_sim
    elif len(consequents) == 1:
        lst = related_similarity_content_tfidf(data_model, input_url, df_recomendation)
        lst_sim = [r for r in lst if r not in consequents]
        results_product = consequents + lst_sim
    elif len(consequents) == 2:
        lst = related_similarity_content_tfidf(data_model, input_url, df_recomendation)
        lst_sim = [r for r in lst if r not in consequents]
        results_product = consequents + lst_sim
    else:
        results_product = consequents
        
    result = {"related_product": []}
    
    for r in results_product:
        url = r.replace('/th/', '/').replace('/en/', '/')
        query = doc_ref.where(u'url', u'==', url).get()[0]
        if query:
            q = query.to_dict()
            del q['createdBy']
            del q['lastModified']
            del q['createdDate']
            del q['modifiedBy']
            result['related_product'].append(q)
        if result['related_product'].__len__() == 3:
            break
        
    return JSONResponse(content=result)
    
@app.get("/")
async def test():
    return "Recommendation Product by Mintel."
