import pandas as pd
import re
import hunt_risk


def get_infection_article():
    """Select articles related ton infection complication and save them in a parquet file"""

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
    """Load articles from infection file and assign a single type for each oh them among
    the following:

        - prospective
        - retrospective
        - meta analysis
        - rct
        - review
        - undefined
    """

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
                    pmid_to_status[pmid] = "prospective"
                elif re.search("retrospective", elt.lower()):
                    pmid_to_status[pmid] = "retrospective"

        if pmid not in pmid_to_status:
            if re.search("prospective", title.lower()):
                pmid_to_status[pmid] = "prospective"
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
    """Load pmid from infection file and assign factors to each of them"""

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


def craft_table():
    """Craft infection table, generate 2 files:

    - infection_table.csv
    - infection_table.html

    """

    # parameters
    factor_to_target = {
        "Advanced Age": [" age ", "old", "older", "young", "elderly"],
        "Chemotherapy": [
            "chemotherapy",
            "fluoropyrimidine",
            "etoposide",
            "carboplatin",
            "docetaxel",
        ],
        "Two or more lumens": [
            "dual-lumen",
            "lumens",
            "multiple-lumen",
            "multilumen",
            "triple lumen",
            "triple-lumen",
            "quadruple lumen",
            "quadruple-lumen",
        ],
        "Antibiotic-coated catheters": ["impregnated", "coated"],
        "Previous PICC": ["previous picc placement", "previous peripherally inserted"],
        "Antibiotic therapy": [
            "anti-bacterial",
            "antifungal",
            "anti-infective",
            "antimicrobial",
            "antibiotic",
        ],
        "Parenteral nutrition": ["parenteral nutrition"],
        "Materials of PICC": ["polyurethanes", "silicones"],
        "PICC dwell time": [
            "catheter day",
            "duration of picc",
            "duration of peripherally",
            "prolonged maintenance",
            "catheter retention time",
            "indwelling time",
        ],
        "Medical department admission (including ICU)": [
            "intensive care unit",
            "medical department admission",
            "admission to",
            "ICU",
        ],
        "Acute myeloid leukemia": ["leukemia"],
        "Auto/allograft": ["autograft", "allograft"],
        "Anticoagulants therapy": ["anticoagulant", "anti-coagulant"],
        "Immunosuppression": ["immunosuppression", " aids", "immune function"],
        "Hospital length of stay": ["hospital length of stay", "length of stay"],
        "Seasonality (summer / warm)": ["season", "summer", "warm"],
        "Operator experience": [
            "experience of operator",
            "experience of picc operator",
        ],
        "Postplacement": ["tip position", "tip malposition"],
        "Fixation device": ["statlock", "fixation device"],
        "Catheter care delay": ["catheter care delay"],
        "Power-injectable piccs": ["power-injectable", "power injectable"],
    }

    # get status
    pmid_to_status = assign_article_type()

    # get factors
    pmid_to_factors = assign_factors()

    # get text
    pmid_to_text = hunt_risk.get_pmid_to_text("data/infection_article.parquet")

    # clean factors
    pmid_to_factors = hunt_risk.assgin_risk_factor(
        pmid_to_factors, pmid_to_text, factor_to_target
    )

    # extract list of factors
    factor_list = []
    for fl in pmid_to_factors.values():
        for f in fl:
            if f not in factor_list:
                factor_list.append(f)

    # extract list of status
    status_list = []
    for s in pmid_to_status.values():
        if s not in status_list:
            status_list.append(s)

    # write table
    matrix = []
    for factor in factor_list:

        vector = {"Factor": factor}
        pmid_associated_to_factor = []
        for pmid in pmid_to_factors:
            if factor in pmid_to_factors[pmid]:
                pmid_associated_to_factor.append(pmid)

        # fill status
        for status in status_list:
            status_line = ""
            for pmid in pmid_associated_to_factor:
                if pmid_to_status[pmid] == status:
                    status_line += f"{pmid} - "
            status_line = status_line[:-2]
            vector[status] = status_line

        matrix.append(vector)

    df = pd.DataFrame.from_dict(matrix)
    df.to_csv("tables/infection_table.csv", index=False)
    df.index = df["Factor"]
    df = df.drop(columns=["Factor"])
    df.to_html("tables/infection_table.tmp")

    # engance table
    style = """
    <style>
    #machin {
      font-family: Arial, Helvetica, sans-serif;
      width: 100%;
    }

    #machin td, #customers th {
      border: 1px solid #ddd;
      padding: 8px;
    }

    #machin tr:nth-child(even){background-color: #f2f2f2;}

    #machin tr:hover {background-color: #ddd;}

    #machin th {
      padding-top: 12px;
      padding-bottom: 12px;
      text-align: center;
      border: 10px solid #ddd;
      background-color: #04AA6D;
      color: white;
    }
    </style>
    """

    table_data = open("tables/infection_table.html", "w")
    table_data.write(style)
    tmp_table = open("tables/infection_table.tmp", "r")
    for line in tmp_table:
        if re.search("^<table ", line):
            line = "<table id='machin'>\n"
        elif re.search("[0-9]{8}", line):
            for elt in re.findall("([0-9]{8})", line):
                line = line.replace(
                    elt, f"<a href='https://pubmed.ncbi.nlm.nih.gov/{elt}/'>{elt}</a>"
                )
        table_data.write(line)
    table_data.close()


if __name__ == "__main__":

    # get_infection_article()
    # check()

    # assign_factors()

    craft_table()
