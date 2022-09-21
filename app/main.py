from typing import Union
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import numpy as np
from utils import related_similarity_content_tfidf
from google.cloud import firestore
from fastapi.middleware.cors import CORSMiddleware
from urllib.parse import quote, unquote
# import numpy as np
# from tqdm.auto import tqdm
# import torch

# #datasets
# from datasets import load_dataset

# #transformers
# from transformers import (
#     CamembertTokenizer,
#     AutoTokenizer,
#     AutoModel,
#     AutoModelForMaskedLM,
#     AutoModelForSequenceClassification,
#     AutoModelForTokenClassification,
#     TrainingArguments,
#     Trainer,
#     pipeline,
# )

# #thai2transformers
# import thai2transformers
# from thai2transformers.preprocess import process_transformers
# from thai2transformers.metrics import (
#     classification_metrics, 
#     multilabel_classification_metrics,
# )
# from thai2transformers.tokenizers import (
#     ThaiRobertaTokenizer,
#     ThaiWordsNewmmTokenizer,
#     ThaiWordsSyllableTokenizer,
#     FakeSefrCutTokenizer,
#     SEFR_SPLIT_TOKEN
# )

df_recomendation = pd.read_csv('gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/recprod_master.csv')
# model_names = [
#     'wangchanberta-base-att-spm-uncased',
#     'xlm-roberta-base',
#     'bert-base-multilingual-cased',
#     'wangchanberta-base-wiki-newmm',
#     'wangchanberta-base-wiki-ssg',
#     'wangchanberta-base-wiki-sefr',
#     'wangchanberta-base-wiki-spm',
# ]

# tokenizers = {
#     'wangchanberta-base-att-spm-uncased': AutoTokenizer,
#     'xlm-roberta-base': AutoTokenizer,
#     'bert-base-multilingual-cased': AutoTokenizer,
#     'wangchanberta-base-wiki-newmm': ThaiWordsNewmmTokenizer,
#     'wangchanberta-base-wiki-ssg': ThaiWordsSyllableTokenizer,
#     'wangchanberta-base-wiki-sefr': FakeSefrCutTokenizer,
#     'wangchanberta-base-wiki-spm': ThaiRobertaTokenizer,
# }
# public_models = ['xlm-roberta-base', 'bert-base-multilingual-cased'] 
# #@title Choose Pretrained Model
# model_name = "wangchanberta-base-att-spm-uncased" #@param ["wangchanberta-base-att-spm-uncased", "xlm-roberta-base", "bert-base-multilingual-cased", "wangchanberta-base-wiki-newmm", "wangchanberta-base-wiki-syllable", "wangchanberta-base-wiki-sefr", "wangchanberta-base-wiki-spm"]

# #create tokenizer
# tokenizer = tokenizers[model_name].from_pretrained(
#                 f'airesearch/{model_name}' if model_name not in public_models else f'{model_name}',
#                 revision='main',
#                 model_max_length=416,)
# zero_classify = pipeline(task='zero-shot-classification',
#          tokenizer=tokenizer,
#          model=AutoModelForSequenceClassification.from_pretrained(
#              f'airesearch/{model_name}' if model_name not in public_models else f'airesearch/{model_name}-finetuned',
#              revision='finetuned@xnli_th')
#          )


# data_model = pd.read_csv('data/df_associations.csv')


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
async def get_related(url: str, cookie: str, cx_Name: str = '', tag: str = '', category: str = '', subItem: str = ''):
    db = firestore.Client()
    prod_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/krungsriProduct/data')
    art_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/articleContent/data')
    
    input_url = url
    input_url = unquote(input_url)
    text = input_url.split('/')[-1]
    df_result = df_recomendation.loc[df_recomendation.link.str.contains(text)]
    # if df_result.__len__() <= 0:
    #     arts = list(art_ref.stream())
    #     arts_dict = list(map(lambda x: x.to_dict(), arts))
    #     df = pd.DataFrame(arts_dict)
    #     result = {'out_labels': [], 'out_scores': []}
    #     try:
    #         keys = tag.split('|')
    #     except AttributeError:
    #         keys = [category, subItem]
    #     for des in prod.description.values:
    #         out = zero_classify(des, 
    #               candidate_labels=keys)
    #         for k in keys:
    #             result['out_labels'].append(des)
    #         result['out_scores'] += out['scores']
    #     res_df = []
    #     scores_test = result['out_scores'].copy()
    #     labels_test = result['out_labels'].copy()
    #     while True:
    #         idx = scores_test.index(max(scores_test))
    #         label = labels_test[idx]
    #         score = scores_test[idx]
    #         scores_test.pop(idx)
    #         labels_test.pop(idx)
    #         if label not in res_df and label in prod.description.values.tolist():
    #             res_df.append(label)
    #         if res_df.__len__() >= 3:
    #             break
    #     new_list = [input_url, cx_Name] + res_df
    #     new_df = pd.DataFrame([new_list], columns=df_recomendation.columns)
    #     df_recomendation = df_recomendation.append(new_df)
    #     df_recomendation = df_recomendation.reset_index(drop=True)
    
    des_list = df_result.values.tolist()[0][2:]
    result = {"related_products": []}
    query = prod_ref.where(u'description', u'in', des_list).get()
    for qry in query:
        q = qry.to_dict()
        del q['createdBy']
        del q['lastModified']
        del q['createdDate']
        del q['modifiedBy']
        del q['productDescription']
        result['related_products'].append(q)
        # result = {'status': 400}
#     df_query = data_model.loc[data_model['antecedents'] == input_url]
#     df_final = df_query.sort_values('confidence', ascending=False)
#     results_product = []
#     consequents = df_final['consequents'].values.tolist()
    
#     result = {"related_product": []}
#     count = 0
#     while True:
#         for cons in consequents:
#             url = cons.replace('/th/', '/').replace('/en/', '/')
#             # print(url)
#             query = doc_ref.where(u'url', u'==', url).get()
#             if query:
#                 q = query[0].to_dict()
#                 del q['createdBy']
#                 del q['lastModified']
#                 del q['createdDate']
#                 del q['modifiedBy']
#                 if q not in result['related_product']:
#                     result['related_product'].append(q)
#             if result['related_product'].__len__() == 3:
#                 break
#         if result['related_product'].__len__() == 3:
#             break
#         else:
#             lst = related_similarity_content_tfidf(data_model, input_url, df_recomendation)
#             df_rec = df_recomendation.loc[df_recomendation.index == int(lst[count])]
#             df_query = data_model.loc[data_model['antecedents'] == f'{df_rec.link.values[0]}']
#             df_final = df_query.sort_values('confidence', ascending=False)
#             consequents = df_final['consequents'].values.tolist()
#             count += 1
        #     consequents = [r for r in lst if r not in result['related_product']]
        # if count == 10:
        #     break
        
    return JSONResponse(content=result)
    
@app.get("/")
async def test():
    return "Recommendation Product by Mintel."
