import pandas as pd
import numpy as np
import pythainlp
from scipy.spatial.distance import pdist,squareform
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer

thai_char = ['ก', 'ข', 'ฃ', 'ค', 'ฅ', 'ฆ', 'ง', 'จ', 'ฉ', 'ช', 'ซ', 'ฌ', 'ญ', 'ฎ', 'ฏ', 'ฐ', 'ฑ', 'ฒ', 'ณ', 'ด', 'ต', 'ถ', 'ท', 'ธ', 'น', 'บ', 'ป', 'ผ', 'ฝ', 'พ', 'ฟ', 'ภ', 'ม', 'ย', 'ร', 'ฤ', 'ล', 'ฦ', 'ว', 'ศ', 'ษ', 'ส', 'ห', 'ฬ', 'อ', 'ฮ', 'ฯ', 'ะ', 'ั', 'า', 'ำ', 'ิ', 'ี', 'ึ', 'ื', 'ุ', 'ู', 'ฺ', '฿', 'เ', 'แ', 'โ', 'ใ', 'ไ', 'ๅ', 'ๆ', '็', '่', '้', '๊', '๋', '์', 'ํ', '๎', '๏', '๐', '๑', '๒', '๓', '๔', '๕', '๖', '๗', '๘', '๙', '๚', '๛']
eng_char = ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]

def deepcut(df):
    deepcut_tokenizer = pythainlp.tokenize.Tokenizer(engine = 'deepcut')
    tfIdf = TfidfVectorizer(tokenizer=deepcut_tokenizer.word_tokenize)
    import deepcut
    countVec = CountVectorizer(tokenizer = deepcut.tokenize,stop_words = thai_char+eng_char)#remove a single character
    CountVector = countVec.fit_transform(df['DocumentPageDescription'].values)
    cVec = pd.DataFrame(CountVector.toarray(),columns = countVec.get_feature_names_out())
    cVec.loc[(cVec[cVec.columns[22]]!=0) | (cVec[cVec.columns[23]]!=0)]
    return cVec


def cosine_related_similarity(df, num_rec=3):
    cosine_matrix = pd.DataFrame(
        squareform(pdist(df,metric = 'cosine')),
        columns = df.index,
        index = df.index
    )
    cosine_matrix.replace({0.0: 1.0}, inplace=True)
    cosine_matrix.to_csv('data/cossim_matrix.csv', index=None)
    a = cosine_matrix.iloc[:, :].to_numpy()
    cosine_matrix['TopThree'] = cosine_matrix.columns[:].to_numpy()[np.argsort(a, axis=1)[:, :num_rec]].tolist()
    return cosine_matrix
