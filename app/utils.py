import pandas as pd

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
