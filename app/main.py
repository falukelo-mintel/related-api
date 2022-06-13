from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
from utils import related_similarity_content
data_model = pd.read_csv('data/df_associations.csv')
df_article = pd.read_csv('data/list_article.csv')

class Item(BaseModel):
    url: str

app = FastAPI()

@app.post("/get_related/")
async def create_item(item: Item):
    input_url = item.url
    df_query = data_model.loc[data_model['antecedents'] == input_url]
    df_final = df_query.sort_values('confidence', ascending=False)[:3]
    results_product = []
    consequents = df_final['consequents'].values.tolist()
    if len(consequents) == 0:
        results_product = related_similarity_content(data_model, input_url, df_article)
    elif len(consequents) == 1:
        lst = related_similarity_content(data_model, input_url, df_article)
        lst_sim = [r for r in lst if r not in consequents][:2]
        results_product = consequents + lst_sim
    elif len(consequents) == 2:
        results_product = consequents
        results_product.append('/'.join(consequents[0].split('/')[:-1]))
    else:
        results_product = consequents
    result = {"related_product": results_product}
    return JSONResponse(content=result)

@app.get("/")
async def test():
    return "Hello Mintel"
