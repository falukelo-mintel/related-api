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

cossim_matrix = pd.read_csv('data/cossim_matrix.csv')
recommended = pd.read_csv('data/recommended.csv')

class Item(BaseModel):
    url: str

app = FastAPI()

@app.post("/get_recommend/")
async def create_item(item: Item):
    input_url = item.url
    input_url = unquote(input_url)
    url = '/'.join(input_url.split('/')[4:])
    description = recommended.loc[recommended['DocumentUrlPath'].str.contains(url)]
    cosine_score = cossim_matrix.values.tolist()
    print(type(cosine_score))
    
    result = {}
    list_recommend = ast.literal_eval(description['recommend'].values[0])
    for i, idx in enumerate(list_recommend):
        score = cosine_score[description.index[0]][idx]
        result[i] = {'url': recommended.iloc[recommended.index == idx]['DocumentUrlPath'].values[0],
                     'similar_score': 1 - score}
    return JSONResponse(content=result)

@app.get("/")
async def test():
    return "Hello Mintel"
