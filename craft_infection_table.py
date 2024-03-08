import pandas as pd
import re


def get_infection_article():
    """ """

    # load data
    df = pd.read_parquet("data/pubmed_filtered.parquet")

    # pick articles
    pmid_to_keep = []
    for index, row in df.iterrows():
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]

        for elt in ot:
            if elt.lower() in ["clasbi", "bsi"]:
                pmid_to_keep.append(pmid)
        for elt in ot:
            if re.search("infection", elt.lower()):
                pmid_to_keep.append(pmid)
        for elt in mh:
            if elt.lower() in ["clasbi", "bsi"]:
                pmid_to_keep.append(pmid)
        for elt in mh:
            if re.search("infection", elt.lower()):
                pmid_to_keep.append(pmid)

        if re.search("infection", title.lower()):
            pmid_to_keep.append(pmid)
        if re.search("infection", abstract.lower()):
            pmid_to_keep.append(pmid)

    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER INFECTION] => {df.shape[0]}")

    df.to_parquet("data/infection_article.parquet")


def assign_article_type():
    """ """

    # load data
    df = pd.read_parquet("data/infection_article.parquet")

    pmid_to_status = {}
    for index, row in df.iterrows():
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]
        article_type = row["TYPE"]

        if "Prospective Studies" in mh:
            pmid_to_status[pmid] = "prospective"
        elif "Retrospective Studies" in mh:
            pmid_to_status[pmid] = "retrospective"

        if pmid not in pmid_to_status:
            for elt in ot:
                if re.search("prospective", elt.lower()):
                    pmid_to_status[pmid] = "prosepective"
                elif re.search("retrospective", elt.lower()):
                    pmid_to_status[pmid] = "retrospective"

        if pmid not in pmid_to_status:
            if re.search("prospective", title.lower()):
                pmid_to_status[pmid] = "prosepective"
            elif re.search("retrospective", title.lower()):
                pmid_to_status[pmid] = "retrospective"

        if pmid not in pmid_to_status:
            if re.search("prospectiv", abstract.lower()):
                pmid_to_status[pmid] = "prospective"
            elif re.search("retrospectiv", abstract.lower()):
                pmid_to_status[pmid] = "retrospective"

        if pmid not in pmid_to_status:
            if "Meta-Analysis" in article_type:
                pmid_to_status[pmid] = "meta-analysis"
            elif "Randomized Controlled Trial" in article_type:
                pmid_to_status[pmid] = "rct"
            elif "Review" in article_type:
                pmid_to_status[pmid] = "review"

        if pmid not in pmid_to_status:
            pmid_to_status[pmid] = "undefined"

    return pmid_to_status


def assign_factors():
    """ """

    # load data
    df = pd.read_parquet("data/infection_article.parquet")

    factor_list = [
        "Advanced age",
        "Chemotherapy",
        "Two or more lumens",
        "Antibiotic-coated catheters",
        "Previous PICC",
        "Antibiotic therapy",
        "Parenteral nutrition",
        "Materials of PICC",
        "PICC dwell time ",
        "Medical department admission (including ICU)",
        "Acute myeloid leukemia",
        "Auto/allograft",
        "Anticoagulants therapy",
        "Immunosuppression",
        "Hospital length of stay",
        "Seasonality (summer / warm)",
        "Operator experience",
        "Postplacement",
        "Fixation device",
        "Catheter care delay",
        "Power-injectable piccs",
    ]

    pmid_to_factor_list = {}
    for index, row in df.iterrows():

        # grab info
        pmid = row["PMID"]
        mh = row["MH"]
        ot = row["OT"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]
        article_type = row["TYPE"]

        # init
        pmid_to_factor_list[pmid] = []

        # catch age
        for elt in mh:
            if elt in ["Aged"] and "Advanced Age" not in pmid_to_factor_list[pmid]:
                pmid_to_factor_list[pmid].append("Advanced Age")

        # catch Chemotherapy
        for elt in mh:
            if re.search("chemotherapy", elt.lower()):
                if "Chemotherapy" not in pmid_to_factor_list[pmid]:
                    pmid_to_factor_list[pmid].append("Chemotherapy")
        for elt in ot:
            if re.search("chemotherapy", elt.lower()):
                if "Chemotherapy" not in pmid_to_factor_list[pmid]:
                    pmid_to_factor_list[pmid].append("Chemotherapy")
        if (
            re.search("chemotherapy", title.lower())
            and "Chemotherapy" not in pmid_to_factor_list[pmid]
        ):
            pmid_to_factor_list[pmid].append("Chemotherapy")

        # catch lumens
        target_list = [
            "dual-lumen",
            "lumens",
            "multiple-lumen",
            "multilumen",
            "triple lumen",
            "triple-lumen",
            "quadruple lumen",
            "quadruple-lumen",
        ]
        for target in target_list:
            if re.search(target, title.lower()) or re.search(target, abstract.lower()):
                if "Two or more lumens" not in pmid_to_factor_list[pmid]:
                    pmid_to_factor_list[pmid].append("Two or more lumens")

        # catch "Antibiotic-coated catheters"
        target_list = [
            "impregnated",
            "coated",
        ]
        tag = "Antibiotic-coated catheters"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch "Previous PICC"
        target_list = ["previous picc placement", "previous peripherally inserted"]
        tag = "Previous PICC"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch Antibiotic therapy
        target_list = [
            "anti-bacterial",
            "antifungal",
            "anti-infective",
            "antimicrobial",
            "antibiotic",
        ]
        tag = "Antibiotic therapy"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch Parenteral nutrition
        target_list = ["parenteral nutrition"]
        tag = "Parenteral nutrition"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch Materials of PICC
        target_list = ["polyurethanes", "silicones"]
        tag = "Materials of PICC"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch PICC dwell time
        target_list = [
            "catheter day",
            "duration of picc",
            "duration of peripherally",
            "prolonged maintenance",
            "catheter retention time",
            "indwelling time",
        ]
        tag = "PICC dwell time"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch Medical department admission (including ICU)
        target_list = [
            "intensive care unit",
            "medical department admission",
            "admission to",
        ]
        tag = "Medical department admission (including ICU)"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["leukemia"]
        tag = "Acute myeloid leukemia"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["autograft", "allograft"]
        tag = "Auto/allograft"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["anticoagulant", "anti-coagulant"]
        tag = "Anticoagulants therapy"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["immunosuppression", " aids", "immune function"]
        tag = "Immunosuppression"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # catch Hospital length
        target_list = ["hospital length of stay", "length of stay"]
        tag = "Hospital length of stay"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["season", "summer", "warm"]
        tag = "Seasonality (summer / warm)"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["experience of operator", "experience of picc operator"]
        tag = "Operator experience"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        target_list = ["tip position", "tip malposition"]
        tag = "Postplacement"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # deat with "Fixation device"
        target_list = ["statlock"]
        tag = "Fixation device"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # Deal with Catheter care delay
        target_list = ["catheter care delay"]
        tag = "Catheter care delay"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

        # Deal with Power-injectable piccs
        target_list = ["power-injectable", "power injectable"]
        tag = "Power-injectable piccs"
        for target in target_list:
            for elt in mh:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            for elt in ot:
                if (
                    re.search(target, elt.lower())
                    and tag not in pmid_to_factor_list[pmid]
                ):
                    pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, title.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)
            if (
                re.search(target, abstract.lower())
                and tag not in pmid_to_factor_list[pmid]
            ):
                pmid_to_factor_list[pmid].append(tag)

    return pmid_to_factor_list


def check():
    """ """

    # load filtered data
    df = pd.read_parquet("data/infection_article.parquet")

    # load check_list for table
    check_list = pd.read_csv("data/check_table.csv")
    check_list = check_list[check_list["COMPLICATION"] == "Infection"]
    check_list = list(check_list["PMID"])
    cmpt = 0
    for pmid in set(check_list):
        if str(pmid) in list(df["PMID"]):
            cmpt += 1
    print(f"[TABLE]{cmpt} / {len(set(check_list))}")


if __name__ == "__main__":

    # get_infection_article()
    # check()

    assign_factors()