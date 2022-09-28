import pandas as pd
import numpy as np
import pythainlp
import time
from scipy.spatial.distance import pdist,squareform
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from pythainlp.tokenize import word_tokenize
from google.cloud import storage
from google.cloud import logging
import logging as log
from google.cloud import storage
import json
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from scipy.spatial.distance import pdist,squareform
import numpy as np
from pythainlp.corpus import thai_stopwords
import re

thai_char = ['ก', 'ข', 'ฃ', 'ค', 'ฅ', 'ฆ', 'ง', 'จ', 'ฉ', 'ช', 'ซ', 'ฌ', 'ญ', 'ฎ', 'ฏ', 'ฐ', 'ฑ', 'ฒ', 'ณ', 'ด', 'ต', 'ถ', 'ท', 'ธ', 'น', 'บ', 'ป', 'ผ', 'ฝ', 'พ', 'ฟ', 'ภ', 'ม', 'ย', 'ร', 'ฤ', 'ล', 'ฦ', 'ว', 'ศ', 'ษ', 'ส', 'ห', 'ฬ', 'อ', 'ฮ', 'ฯ', 'ะ', 'ั', 'า', 'ำ', 'ิ', 'ี', 'ึ', 'ื', 'ุ', 'ู', 'ฺ', '฿', 'เ', 'แ', 'โ', 'ใ', 'ไ', 'ๅ', 'ๆ', '็', '่', '้', '๊', '๋', '์', 'ํ', '๎', '๏', '๐', '๑', '๒', '๓', '๔', '๕', '๖', '๗', '๘', '๙', '๚', '๛']
eng_char = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]

# def deepcut(df):
#     deepcut_tokenizer = pythainlp.tokenize.Tokenizer(engine = 'deepcut')
#     tfIdf = TfidfVectorizer(tokenizer=deepcut_tokenizer.word_tokenize)
#     import deepcut
#     countVec = CountVectorizer(tokenizer = deepcut.tokenize,stop_words = thai_char+eng_char)#remove a single character
#     CountVector = countVec.fit_transform(df['DocumentPageDescription'].values)
#     cVec = pd.DataFrame(CountVector.toarray(),columns = countVec.get_feature_names_out())
#     cVec.loc[(cVec[cVec.columns[22]]!=0) | (cVec[cVec.columns[23]]!=0)]
#     return cVec


# def cosine_related_similarity(df, num_rec=3):
#     cosine_matrix = pd.DataFrame(
#         squareform(pdist(df,metric = 'cosine')),
#         columns = df.index,
#         index = df.index
#     )
#     cosine_matrix.replace({0.0: 1.0}, inplace=True)
#     cosine_matrix.to_csv('data/cossim_matrix.csv', index=None)
#     a = cosine_matrix.iloc[:, :].to_numpy()
#     cosine_matrix['TopThree'] = cosine_matrix.columns[:].to_numpy()[np.argsort(a, axis=1)[:, :num_rec]].tolist()
#     return cosine_matrix

def text_tokenizer(text):
    terms = [k.strip() for k in word_tokenize(text, engine='nercut') if len(k.strip()) > 0 and k.strip() not in stop_words]

    return [t for t in terms if len(t) > 0 or t is not None]

def text_processor(text):
    text = re.sub('ํา','ำ', text).lower()
    text = re.sub('เเ','แ', text).lower()
    text = re.sub('[^A-Za-z0-9ก-๙\s]+|ๆ', '', text)
    return text

def train_rec(df, fliename):
    stop_words = [t for t in list(thai_stopwords())]
    tfidf_vectors = TfidfVectorizer(
        tokenizer=text_tokenizer,
        preprocessor=text_processor,
        ngram_range=(1, 1),
        stop_words=stop_words,
        min_df=int(df.shape[0] * 0.01),
        max_features=5_000
    )
    X = tfidf_vectors.fit_transform(df.textContent)
    terms_tfidf = pd.DataFrame(
        X.toarray(),
        columns=tfidf_vectors.get_feature_names(),
        index=df.index,

    )

    cossim_matrix = pd.DataFrame(
        squareform(pdist(terms_tfidf,metric = 'cosine')),
        columns = terms_tfidf.index,
        index = terms_tfidf.index
    )
    cossim_matrix.replace({0.0: 1.0}, inplace=True)
    a = cossim_matrix.iloc[:, :].to_numpy()
    cossim_matrix['recommend'] = cossim_matrix.columns[:].to_numpy()[np.argsort(a, axis=1)[:, :30]].tolist()
    df['recommend'] = cossim_matrix['recommend']
    df = df[['link', 'recommend']]
    blob_source_name = f'Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/{fliename}.csv'
    source_blob = bucket.blob(blob_source_name)
    if source_blob.exists():
        blob_dest_name = f'Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/tmp/{fliename}-{str(time.time_ns())}.csv'

        new_blob = bucket.copy_blob(
            source_blob, bucket, blob_dest_name)
        source_blob.delete()
        print('Move data model to /tmp/')
    else:
        print(f'Source blob [{blob_source_name}] not exists.')
    df.to_csv(f'gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/model/{fliename}.csv', index=None)