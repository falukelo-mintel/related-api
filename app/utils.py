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