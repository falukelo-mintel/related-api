from sklearn.feature_extraction.text import TfidfVectorizer

import pandas as pd
from pythainlp import word_tokenize
from pythainlp.corpus.common import thai_stopwords
import re
import pickle

def preprocess_text(text):
    text = text.replace('ํา','ำ')
    text = text.replace('เเ','แ')
    text = re.sub('http://\S+|https://\S+', '', text)
    text = re.sub('[^A-Za-zก-์\s]+|ๆ', '', text)
    return text

def text_tokenizer(text):
    stop_words = [t for t in list(thai_stopwords())]
    terms = [k.strip() for k in word_tokenize(text,keep_whitespace=False, engine='nercut') if len(k.strip()) > 0 and k.strip() not in stop_words]

    return [t for t in terms if len(t) > 0 or t is not None]

def tfidf_vectorize(df):
    stop_words = [t for t in list(thai_stopwords())]
    tfidf_model = TfidfVectorizer( 
                        preprocessor=preprocess_text, # Remove unwanted characters 
                        tokenizer=text_tokenizer, # Add Wrap Analyzer 
                        stop_words=stop_words, # Enter a prepared list of Stop words 
                        ngram_range=(2, 3), # Want to analyze 2 and 3 consecutive words 
                        min_df=20, # Minimum Doc Freq of Term 
                        max_features=500 # use only the first 3,000 Term 
                    )
    
    #pickle.dump(tfidf_model, open("tfidf_model_use.pickle", "wb")) # First vectorizer, this is currently being used
    
    a = tfidf_model.fit_transform(df.textContent)
    a_terms = pd.DataFrame(a.toarray(),columns = tfidf_model.get_feature_names(),index = df.index)
    
    return a_terms
