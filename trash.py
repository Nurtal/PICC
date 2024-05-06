import pandas as pd


def reformat_check_table():
    """ """

    # load data
    df = pd.read_csv("data/check_thrombosis_fusion.csv")

    data = []
    for index, row in df.iterrows():

        factor = row["FACTOR"]
        pmid_list = row["PMID_LIST"].split(";")
        pmid_list_clean = []
        pmid_str = ""
        for pmid in pmid_list:
            if pmid not in pmid_list_clean:
                pmid_list_clean.append(pmid)
                pmid_str += str(pmid) + ";"
        pmid_str = pmid_str[:-1]

        data.append({"FACTOR": factor, "PMID_LIST": pmid_str})

    df = pd.DataFrame.from_dict(data)
    df.to_csv("data/check_thrombosis_fusion_cleaned.csv", index=False)


if __name__ == "__main__":

    reformat_check_table()
