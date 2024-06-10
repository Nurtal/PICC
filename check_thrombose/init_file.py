import pandas as pd

def init_file():
    """ """


    df = pd.read_csv('../tables/thrombose_table.csv')
    factor_list = list(df['Factor'])
    df = pd.read_parquet("../data/thrombose_article.parquet") 
    pmid_list = list(df['PMID'])

    data = []
    for pmid in pmid_list:
        vector = {'PMID': pmid}
        for factor in factor_list:
            vector[factor] = ""
        data.append(vector)

    df = pd.DataFrame.from_dict(data)
    df.to_csv('thrombose.csv', index=False)

if __name__ == "__main__":


    init_file()

