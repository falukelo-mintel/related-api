from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
from google.cloud import firestore
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import numpy as np
from urllib.parse import quote, unquote
from DataPreparation import *
from TFIDF import *
from Segmentation import *

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

@app.get("/Train_Product/")
async def TrainProd():
    
    ProductClustering()
    CleanUserData()
        
    return True

@app.get("/Train_User/")
async def TrainUser():
    
    SegmentUsers()
        
    return True
    

@app.get("/")
async def test():
    return "Recommendation Product by Mintel."
