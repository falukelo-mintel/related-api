import pandas as pd
from datetime import date
from nanoid import generate
from google.cloud import firestore
# import firebase_admin
# from firebase_admin import credentials, firestore

tag_name = {
    0:'All about Krungsri',
    1:'Out Scope 1',
    2:'Insurance',
    3:'Card',
    4:'Business',
    5:'Personal Loan',
    6:'Home Loan',
    7:'Travel',
    8:'Investment',
    9:'Tax',
    10:'Online marketing',
    11:'Auto Loan',
    12:'Technology',
    13:'Personal Finance',
    14:'Health insurance',
    15:'Home Refinance',
    16:'Condo Investment',
    17:'Home Loan Interest',
    18:'Tax Planner',
    19:'Commerce',
    20:'Portfolio',
    21:'Life saving',
    22:'Out scope 2',
    23:'Horoscope',
    24:'Self learning',
    25: 'Out scope 3'
}

def update():
    filename_User = "gs://connect-x-production.appspot.com/Organizes/pJoo5lLhhAbbofIfYdLz/AI/UserSegments/UserSegments.csv"
    df = pd.read_csv(filename_User)
    df = df[['unknownContact', 'Topic']]
    df = df.drop_duplicates()
    
    db = firestore.Client()
    tag_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/tag/data')
    unk_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/unknownContact/data')
    tags = {}
    for key in tag_name:
        query_doc = tag_ref.where(u'cx_Name', u'==', tag_name[key]).get()
        tag_id = query_doc[0].id
        tags[tag_name[key]] = tag_id
        
    for row in df.iterrows():
        try:
            topic = int(row[1]['Topic'])
            assert topic != -1
        except ValueError:
            topic = 0
        except AssertionError:
            topic = 25
        topic = tag_name[topic]
        tag_id = tags[topic]
        unknown_ref = unk_ref.document(row[1]['unknownContact'])
        data = unknown_ref.get()
        if data.exists:
            unk_data = data.to_dict()
            try:
                data = {
                    'tag': unk_data['tag']
                }
            except:
                data = {
                    'tag': []
                }
            data['lastModified'] = firestore.SERVER_TIMESTAMP
            if tag_id not in data['tag']:
                unknown_ref.update(data)
                tag_doc = tag_ref.document(tag_id)
                current_count = tag_doc.get().to_dict()['cx_count']
                tag_doc.update({
                    u'cx_count': int(current_count) + 1,
                    u'lastModified': firestore.SERVER_TIMESTAMP
                })