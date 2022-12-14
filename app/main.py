from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
from google.cloud import firestore
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import numpy as np
from urllib.parse import quote, unquote
from UserDataPrep import *
from UserSegmentation import *
from UpdateTags import *

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

@app.get("/Clean_User_Data/")
async def CleanUser():
    CleanUserData()
    return True

@app.get("/Map_To_User/")
async def TrainUser():
    MapToUser()
    return True

@app.get("/Update_Tag/")
async def TrainUser():
    update()
    return True
    

@app.get("/")
async def test():
    return "Recommendation Product by Mintel."
