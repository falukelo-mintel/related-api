def check_path2(url):
    url2 = url.split('/')
    if (url2[-1]=='personal'):
        return False
    
    cat = url.split('personal/')
    if len(cat) >2: 
        return False
    cat = cat[1]
    cat = cat.split('/')[0]
    return cat

def check_path(url):
    cata = ['deposit','loans','card','bancassurance','mutual-fund','banking-services','digital-banking']
    if url[-1] != '/':
        url = url+'/'
    for item in cata:
        if f'personal/{item}/' in url:
            return True
    return False

def check_score(pdt,url):
    if pdt == 'product':
        return 5
    else:
        if url.find('th/personal/') != -1 or url.find('en/personal/') != -1:
            next_url = url.split('/')
            if(next_url[len(next_url)-1] != 'personal'):
                return len(next_url)-next_url.index('personal')
            else:
                return -1
        return -1
    
def update_tag_unknown(db, cx_cookie, seg):
    doc_ref = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/activities/data')
    doc_unknown = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/unknownContact/data')
    doc_tag = db.collection(u'Organizes/pJoo5lLhhAbbofIfYdLz/objects/tag/data')
    query = doc_ref.where(u'cx_cookie', u'==', cx_cookie).get()
    for q in query:
        activity = q.to_dict()
        value = activity['unknownContact']['value']
        query_doc = doc_tag.where(u'cx_Name', u'==', seg).get()
        tag_id = query_doc[0].id
        unknown_ref = doc_unknown.document(value)
        unknown_data = unknown_ref.get().to_dict()
        unknown_tag = unknown_data['tag']
        unknown_tag.append(tag_id)
        unknown_ref.update({
            u'tag': unknown_tag,
            u'lastModified': firestore.SERVER_TIMESTAMP
        })
        tag_ref = doc_tag.document(tag_id)
        current_count = tag_ref.get().to_dict()['cx_count']
        tag_ref.update({
            u'cx_count': current_count + 1,
            u'lastModified': firestore.SERVER_TIMESTAMP
        })