from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
from utils import related_similarity_content_tfidf
from google.cloud import firestore
from fastapi.middleware.cors import CORSMiddleware
data_model = pd.read_csv('data/df_associations.csv')
recommended = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recommended.csv')

# class Item(BaseModel):
#     url: str
#     cookie: str

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/get_related/")
async def get_related(url: str, cookie: str):
    db = firestore.Client()
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/krungsriProduct/data')
    input_url = url
    df_query = data_model.loc[data_model['antecedents'] == input_url]
    df_final = df_query.sort_values('confidence', ascending=False)
    results_product = []
    consequents = df_final['consequents'].values.tolist()
    
    result = {"related_product": []}
    count = 0
    while True:
        for cons in consequents:
            url = cons.replace('/th/', '/').replace('/en/', '/')
            # print(url)
            query = doc_ref.where(u'url', u'==', url).get()
            if query:
                q = query[0].to_dict()
                del q['createdBy']
                del q['lastModified']
                del q['createdDate']
                del q['modifiedBy']
                if q not in result['related_product']:
                    result['related_product'].append(q)
            if result['related_product'].__len__() == 3:
                break
        if result['related_product'].__len__() == 3:
            break
        else:
            lst = related_similarity_content_tfidf(data_model, input_url, df_recomendation)
            df_rec = df_recomendation.loc[df_recomendation.index == lst[count]]
            df_query = data_model.loc[data_model['antecedents'] == f'https://www.krungsri.com/th{df_rec.DocumentUrlPath.values[0]}']
            df_final = df_query.sort_values('confidence', ascending=False)
            consequents = df_final['consequents'].values.tolist()
            count += 1
        #     consequents = [r for r in lst if r not in result['related_product']]
        # if count == 10:
        #     break
        
    return JSONResponse(content=result)
    
@app.get("/")
async def test():
    return "Recommendation Product by Mintel."
