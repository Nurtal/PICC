import pandas as pd
import re


def filter():
    """ """

    # load data
    df = pd.read_parquet("data/pubmed.parquet")
    print(f"[LOAD DATA] => {df.shape[0]}")

    # deal with date
    df["YEAR"] = pd.to_numeric(df["YEAR"])
    df = df[df["YEAR"] >= 2013]
    df = df[df["YEAR"] <= 2023]
    print(f"[FILTER DATE] => {df.shape[0]}")

    # deal with lang
    pmid_to_keep = []
    for index, row in df.iterrows():
        pmid = row["PMID"]
        lang_list = row["LANGUAGE"]
        if "eng" in lang_list or "fra" in lang_list:
            pmid_to_keep.append(pmid)
    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER LANGUAGE] => {df.shape[0]}")

    # deal with type
    pmid_to_keep = []
    for index, row in df.iterrows():
        if "Case Reports" not in row["TYPE"]:
            pmid_to_keep.append(row["PMID"])
    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER CASE REPORT] => {df.shape[0]}")

    # deal with child
    pmid_to_keep = []
    pmid_to_drop = []
    for index, row in df.iterrows():
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        title = row["TITLE"]

        if re.search("neonatal", title.lower()):
            pmid_to_drop.append(pmid)

        try:
            if (
                "Child" not in mh
                and "Neonatal" not in mh
                and "Newborn" not in mh
                and "Infant, Newborn" not in mh
                and "Intensive Care Units, Neonatal" not in mh
                and "Infant, Premature" not in mh
                and "children" not in ot
                and "child" not in ot
                and "pediatrics" not in ot
                and "infants—children" not in ot
                and "Preterm infant" not in ot
                and "premature infants" not in ot
                and "Very low birth weight infants" not in ot
            ):
                pmid_to_keep.append(pmid)
        except:
            pmid_to_keep.append(pmid)
    df = df[df["PMID"].isin(pmid_to_keep)]
    df = df[~df["PMID"].isin(pmid_to_drop)]
    print(f"[FILTER CHILD] => {df.shape[0]}")

    # deal with animals
    pmid_to_keep = []
    for index, row in df.iterrows():
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        try:
            if (
                "Animals" not in mh
                and "Disease Models, Animal" not in mh
                and "Mice" not in mh
                and "Rabbits" not in mh
                and "rabbit VX2 liver tumor model" not in ot
            ):
                pmid_to_keep.append(pmid)
        except:
            pass
    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER ANIMAL] => {df.shape[0]}")

    # deal with complication
    pmid_to_keep = []
    for index, row in df.iterrows():
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]

        if (
            re.search("thrombosis", title.lower())
            or re.search("infection", title.lower())
            or re.search("thromboembolism", title.lower())
        ):
            pmid_to_keep.append(pmid)

        if (
            re.search("bloodstream infection", abstract.lower())
            or re.search("catheter indwelling-related complications", abstract.lower())
            or re.search("catheterization-related complications", abstract.lower())
        ):
            pmid_to_keep.append(pmid)

        for elt in mh:
            if re.search("adverse", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("risk", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("infection", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("thrombosis", elt.lower()):
                pmid_to_keep.append(pmid)

        for elt in ot:
            if re.search("adverse", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("risk", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("infection", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("thrombosis", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("complication", elt.lower()):
                pmid_to_keep.append(pmid)
            elif re.search("CRBSI", elt):
                pmid_to_keep.append(pmid)
            elif re.search("occlusion", elt.lower()):
                pmid_to_keep.append(pmid)

    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER COMPLICATION] => {df.shape[0]}")

    # save data
    df.to_parquet("data/pubmed_filtered.parquet")
    df.to_csv("data/pubmed_filtered.csv", index=False)


def check():
    """ """

    # load filtered data
    df = pd.read_parquet("data/pubmed_filtered.parquet")

    # load check_list
    check_list = list(pd.read_csv("data/check.csv")["PMID"])
    cmpt = 0
    for pmid in check_list:
        if str(pmid) in list(df["PMID"]):
            cmpt += 1
        else:
            print(pmid)
    print(f"[OLD]{cmpt} / {len(check_list)}")

    # load check_list for table
    check_list = list(pd.read_csv("data/check_table.csv")["PMID"])
    cmpt = 0
    for pmid in set(check_list):
        if str(pmid) in list(df["PMID"]):
            cmpt += 1
    print(f"[TABLE]{cmpt} / {len(set(check_list))}")


if __name__ == "__main__":

    filter()
    check()
