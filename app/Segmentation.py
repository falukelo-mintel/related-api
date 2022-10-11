import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Importing own modules
def convertMatrix(df):
    for i in range(6):
        col = 'categ_{}'.format(i)        
        df_t = df[df['cat_product'] == i]
        time_t = df_t['cat_product'] 
        time_t = time_t.apply(lambda x:1 if x > 0 else 1)
        df.loc[:, col] = time_t
        df[col].fillna(0, inplace = True)
    
    df.drop(['cx_web_url_fullpath','cat_product','unknownContact.value','unknownContact.label'], axis = 1,inplace = True)
    
    return df

def SegmentUsers():
    df_cleaned = pd.read_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/state/mappedUsers.csv")
    df_cleaned = convertMatrix(df_cleaned)
    
    #Group by users
    'fingerprint + cookie'
    df_matrix = df_cleaned.groupby('fingerprint + cookie').sum()
    #df_matrix = df_cleaned.groupby('unknownContact.label').sum()
    
    #Converting to matrix for KMeans
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df_matrix.values)
    
    n_clusters = 7
    kmeans = KMeans(init='k-means++', n_clusters = n_clusters, n_init=30, random_state=42, verbose = 0)
    kmeans.fit(scaled)
    user_clusters = kmeans.predict(scaled)
    
    df_matrix.loc[:, 'cluster'] = user_clusters
    
    df_matrix.to_csv("gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/final/UserSegment.csv")
