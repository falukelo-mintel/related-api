import pandas as pd
import ast

def overlapping_sublists(l, n, overlap=0, start=0):          
    while start <= len(l) - n:
        yield l[start:start+n]
        start += n - overlap
        
def get_cateandsub(content_url, df_article):
    content_name = content_url.split('/')[-1]
    df_tmp = df_article.loc[df_article["DocumentUrlPath"].str.contains(content_name, na=False)]
    node_alias_path = df_tmp['NodeAliasPath'].values.tolist()[0]
    url_split = node_alias_path.split('/')
    category = url_split[1]
    sub_category = url_split[2]
    return category, sub_category


def related_similarity_content(df, content_url, df_article):
    category, sub_category = get_cateandsub(content_url, df_article)
    print(category, sub_category)
    df_tmp = df.loc[df["antecedents"].str.contains("/{}/{}".format(category, sub_category), na=False)]
    if len(df_tmp) == 0:
        df_tmp = df.loc[df["antecedents"].str.contains("/{}/".format(category), na=False)]
    if len(df_tmp) == 1:
        result = df_tmp["consequents"].values.tolist()
        df_tmp = df.loc[df["antecedents"].str.contains("/{}/".format(category), na=False)]
        result.append(list(set(df_tmp["consequents"].values.tolist()))[:2])
    if len(df_tmp) == 2:
        result = df_tmp["consequents"].values.tolist()
        df_tmp = df.loc[df["antecedents"].str.contains("/{}/".format(category), na=False)]
        result.append(df_tmp["consequents"].values.tolist()[:1])
    else:
        result = list(set(df_tmp["consequents"].values.tolist()))[:3]
    return result

def related_similarity_content_tfidf(df, content_url, df_recomendation):
    content_name = content_url.split('/')[-1]
    # print(content_name)
    df_rec = df_recomendation.loc[df_recomendation["DocumentUrlPath"].str.contains(content_name, na=False)]
    list_recommend = ast.literal_eval(df_rec['recommend'].values[0])
    # sublist = overlapping_sublists(list_recommend, 4)
    # ss = []
    # for s in sublist:
    #     # ss += s
    #     # print(s)
    #     df_search = df_recomendation.iloc[s]
        # print(df_search['DocumentUrlPath'].values)
        # df_tmp = df.loc[df["antecedents"].str.contains('|'.join(df_search['DocumentUrlPath'].values), na=False)]
        # df_tmp = df_tmp.drop_duplicates(subset=['antecedents'])
        # df_tmp = df_tmp.drop_duplicates(subset=['consequents'])
    # if len(df_tmp) > 3:
        # print(ss)
        # break
#         df_tmp = df_tmp.sort_values(by=['frequency_antecedent', 'confidence'], ascending=False)

#         ss += df_tmp['consequents'].values.tolist()
        # print(ss)
        #     print(ss)
        #     if len(set(ss)) > 3:
        #         break
        # if len(set(ss)) > 3:
        #         break
    return list_recommend
