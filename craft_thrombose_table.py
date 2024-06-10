import pandas as pd
import re
import hunt_risk
import table_check
import extract_information


def get_thrombose_article():
    """Select articles related ton thrombotic complication and save them in a parquet file"""

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
            if re.search("thromb", elt.lower()):
                pmid_to_keep.append(pmid)

        if re.search("thromb", title.lower()):
            pmid_to_keep.append(pmid)
        if re.search("thromb", abstract.lower()):
            pmid_to_keep.append(pmid)

    df = df[df["PMID"].isin(pmid_to_keep)]
    print(f"[FILTER THROMBOSIS] => {df.shape[0]}")

    # debug
    target_pmid = "25673995"
    if target_pmid in list(df["PMID"]):
        print(f"[THROMBOSIS][{target_pmid}] present")

    df.to_parquet("data/thrombose_article.parquet")


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
    df = pd.read_parquet("data/thrombose_article.parquet")

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


def hunt_factors(target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list):
    """ """

    for target in target_list:
        for elt in mh:
            if re.search(target, elt.lower()) and tag not in pmid_to_factor_list[pmid]:
                pmid_to_factor_list[pmid].append(tag)
        for elt in ot:
            if re.search(target, elt.lower()) and tag not in pmid_to_factor_list[pmid]:
                pmid_to_factor_list[pmid].append(tag)
        if re.search(target, title.lower()) and tag not in pmid_to_factor_list[pmid]:
            pmid_to_factor_list[pmid].append(tag)
        if re.search(target, abstract.lower()) and tag not in pmid_to_factor_list[pmid]:
            pmid_to_factor_list[pmid].append(tag)

    return pmid_to_factor_list


def assign_factors():
    """Load pmid from infection file and assign factors to each of them"""

    # load data
    df = pd.read_parquet("data/thrombose_article.parquet")

    factor_list = [
        "Two or more lumens",
        "Power injectable PICC",
        "Immunosuppression",
        "Intensive care unit",
        "Chemotherapy",
        "Diabetes",
        "Cancer",
        "Sex_male",
        "BMI_height_obesity",
        "Smoking_History of smoking",
        "Age",
        "Thrombosis_History of thrombosis_past history of VTE",
        "Large catheter_large gauge catheter_catheter to vein ratio",
        "PICC insertion procedure",
        "PICC indwelling time",
        "Number of comorbidities/HTA/Hyperglycemia",
        "Performans score_performance status_karnofsky_ECOG_Less activity_bedridden > 72h",
        "Total parenteral nutrition",
        "Biology ( blood platelet level_white blood cell level_triglycerides_fibrinogen)",
        "Upper extremity damages",
        "Surgery_type of surgery",
        "Transplantation",
        "High intrathoracic pressure",
        "PICC infection",
        "Score",
        "Menopause_endocrine therapy_hormone use",
        "Erythropoiesis-stimulating agents",
        "Slower blood flow velocity",
        "Non-O blood type_ group B",
        "Anticoagulants therapy",
        "Thrombophilia",
        "Others (COPD, IBD",
        "Arm_laterality _vein choice",
        "Hypertension",
        "Hospitalisation/ Medical department admission/ discharged to a skilled-nursing facility",
        "Blood vessel irritating drug_vancomycin_amphotericine",
    ]

    factor_to_target = {
        "PICC insertion procedure": [
            "insertion procedure",
            "position",
            "picc insertion",
            "operator experience",
            "vein depth",
        ],
        "PICC indwelling time": [
            "catheter day",
            "duration of picc",
            "duration of peripherally",
            "prolonged maintenance",
            "catheter retention time",
            "indwelling time",
        ],
        "Number of comorbidities/HTA/Hyperglycemia": [
            "hta",
            "hyperglycemia",
            "comorbiditie",
        ],
        "Performans score_performance status_karnofsky_ECOG_Less activity_bedridden > 72h": [
            "performance score",
            "performance status",
            "karnofsky",
            "ecog",
            "less activity",
            "bedridden",
        ],
        "Total parenteral nutrition": ["parenteral nutrition"],
        "Biology ( blood platelet level_white blood cell level_triglycerides_fibrinogen)": [
            "platelet",
            "blood cell",
            "triglycerides",
            "fibrinogen",
        ],
        "Upper extremity damages": ["upper extremity damages"],
        "Surgery_type of surgery": ["surgery"],
        "Transplantation": ["transplantation"],
        "High intrathoracic pressure": ["intrathoracic press"],
        "PICC infection": ["infection"],
        "Score": [],
        "Menopause_endocrine therapy_hormone use": [
            "menopause",
            "endocrine",
            "hormone",
        ],
        "Erythropoiesis-stimulating agents": ["erythropoiesis"],
        "Slower blood flow velocity": ["blood flow velocity"],
        "Non-O blood type_ group B": ["blood type"],
        "Anticoagulants therapy": ["anticoagulant", "anti-coagulant"],
        "Thrombophilia": ["thrombophilia"],
        "Others (COPD, IBD)": ["copd", "ibd"],
        "Arm_laterality _vein choice": ["arm laterality", "vein choice"],
        "Hypertension": ["hypertension"],
        "Hospitalisation/ Medical department admission/ discharged to a skilled-nursing facility": [
            "medical department admission",
            "admission to",
        ],
        "Blood vessel irritating drug_vancomycin_amphotericine": [
            "vancomycin",
            "amphotericine",
        ],
    }

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

        # deal with power-injectable
        target_list = ["power-injectable", "power injectable"]
        tag = "Power injectable PICC"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # deal with immunosupression
        target_list = ["immunosuppression", " aids", "immune function"]
        tag = "Immunosuppression"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch ICU
        target_list = ["intensive care unit"]
        tag = "ICU"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch chemotherapy
        target_list = [
            "chemotherapy",
            "fluoropyrimidine",
            "etoposide",
            "carboplatin",
            "docetaxel",
        ]
        tag = "Chemotherapy"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch Diabetes
        target_list = ["diabetes", "diabetic"]
        tag = "Diabete"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch cancer
        target_list = [
            "cancer",
            "carcinoma",
            "metastasis",
            "hematologic malignancy",
            "leukemia",
            "lymphoma",
            "neoplasia",
        ]
        tag = "Cancer"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch gender
        target_list = ["sex", "gender", "male", "female"]
        tag = "Gender"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch obesity
        target_list = ["obesity", "body mass index", "bmi"]
        tag = "BMI"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch smoking
        target_list = ["smoking", "smoker"]
        tag = "Smoking"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch age
        target_list = ["age"]
        tag = "Age"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch history of thrombosis
        target_list = [
            "history of deep venous thrombo",
            "history of thrombo",
            "past thrombo",
        ]
        tag = "History Of Thrombosis"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # catch large catheter
        target_list = [
            "large catheter",
            "large gauge",
            "catheter to vein ratio",
        ]
        tag = "Large catheter"
        pmid_to_factor_list = hunt_factors(
            target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
        )

        # generic solution
        for tag in factor_to_target:
            target_list = factor_to_target[tag]
            pmid_to_factor_list = hunt_factors(
                target_list, tag, mh, ot, title, abstract, pmid, pmid_to_factor_list
            )

    # debug
    target_pmid = "29659465"
    if target_pmid in pmid_to_factor_list:
        print(f"[FACTOR][{target_pmid}] present")

    return pmid_to_factor_list


def craft_table():
    """Craft infection table, generate 2 files:

    - thrombose_table.csv
    - thrombose_table.html

    """

    factor_to_target = {
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
        "Power injectable PICC": ["power-injectable", "power injectable"],
        "Immunosuppression": ["immunosuppression", " aids", "immune function"],
        "ICU": [
            "intensive care unit",
            "medical department admission",
            "admission to",
            "ICU",
        ],
        "Chemotherapy": [
            "chemotherapy",
            "fluoropyrimidine",
            "etoposide",
            "carboplatin",
            "docetaxel",
        ],
        "Diabete": ["diabetes", "diabetic"],
        "Cancer": [
            "cancer",
            "carcinoma",
            "metastasis",
            "hematologic malignancy",
            "leukemia",
            "lymphoma",
            "neoplasia",
        ],
        "Gender": ["sex", "gender", "male", "female"],
        "BMI": ["obesity", "body mass index", "bmi"],
        "Smoking": ["smoking", "smoker"],
        "Age": [" age ", "old", "older", "young", "elderly"],
        "History Of Thrombosis": [
            "history of deep venous thrombo",
            "history of thrombo",
            "past thrombo",
        ],
        "Large catheter": ["Large catheter"],
        "PICC insertion procedure": [
            "insertion procedure",
            "position",
            "picc insertion",
            "operator experience",
            "vein depth",
        ],
        "PICC indwelling time": [
            "catheter day",
            "duration of picc",
            "duration of peripherally",
            "prolonged maintenance",
            "catheter retention time",
            "indwelling time",
        ],
        "Number of comorbidities/HTA/Hyperglycemia": [
            "hta",
            "hyperglycemia",
            "comorbiditie",
        ],
        "Performans score_performance status_karnofsky_ECOG_Less activity_bedridden > 72h": [
            "performance score",
            "performance status",
            "karnofsky",
            "ecog",
            "less activity",
            "bedridden",
        ],
        "Total parenteral nutrition": ["parenteral nutrition"],
        "Biology ( blood platelet level_white blood cell level_triglycerides_fibrinogen)": [
            "platelet",
            "blood cell",
            "triglycerides",
            "fibrinogen",
        ],
        "Upper extremity damages": ["upper extremity damages"],
        "Surgery_type of surgery": ["surgery"],
        "Transplantation": ["transplantation"],
        "High intrathoracic pressure": ["intrathoracic press"],
        "PICC infection": ["infection"],
        "Menopause_endocrine therapy_hormone use": [
            "menopause",
            "endocrine",
            "hormone",
        ],
        "Erythropoiesis-stimulating agents": ["erythropoiesis"],
        "Slower blood flow velocity": ["blood flow velocity"],
        "Non-O blood type_ group B": ["blood type"],
        "Anticoagulants therapy": ["anticoagulant", "anti-coagulant"],
        "Thrombophilia": ["thrombophilia"],
        "Others (COPD, IBD)": ["copd", "ibd"],
        "Arm_laterality _vein choice": ["arm laterality", "vein choice"],
        "Hypertension": ["hypertension"],
        "Hospitalisation/ Medical department admission/ discharged to a skilled-nursing facility": [
            "medical department admission",
            "admission to",
        ],
        "Blood vessel irritating drug_vancomycin_amphotericine": [
            "vancomycin",
            "amphotericine",
        ],
    }

    # get status
    pmid_to_status = assign_article_type()

    # get factors
    pmid_to_factors = assign_factors()

    # get text
    pmid_to_text = hunt_risk.get_pmid_to_text("data/thrombose_article.parquet")

    # clean factors
    pmid_to_factors = hunt_risk.assgin_risk_factor(
        pmid_to_factors, pmid_to_text, factor_to_target
    )

    # print(pmid_to_factors["29659465"])

    # craft data for LLM
    hunt_risk.craft_risk_sentence_dataset(
        pmid_to_factors,
        pmid_to_text,
        factor_to_target,
        "data/thrombose_risk_evaluation.csv",
    )

    # run check
    table_check.check_factors(
        pmid_to_factors,
        "data/check_thrombosis_fusion_cleaned.csv",
        "log/check_thrombose.log",
    )

    # extract verified pmid to factor
    pmid_to_factor_verified = table_check.get_pmid_to_factor(
        "data/check_thrombosis_fusion_cleaned.csv"
    )

    # run LLM to check risk factors
    pmid_to_factors_llm = extract_information.run(
        "data/thrombose_risk_evaluation.csv",
        "data/thrombose_extracted_risk_with_llama3.csv",
        "log/thrombose_extracted_risk_with_llama3.log",
        "thrombosis",
    )

    # Display count
    pmid_only_llm = []
    pmid_both = []
    pmid_only_manual = []
    pmid_neither = []
    for pmid in pmid_to_factors_llm:
        if(pmid in pmid_to_factor_verified):
            pmid_both.append(pmid)
        else:
            pmid_only_llm.append(pmid)

    for pmid in pmid_to_factor_verified:
        if(pmid not in pmid_to_factors_llm):
            pmid_only_manual.append(pmid)

    for pmid in pmid_to_factors:
        if pmid not in pmid_to_factors_llm and pmid not in pmid_to_factor_verified:
            pmid_neither.append(pmid)
    print("")
    print(f"[AI+MANUAL] => {len(pmid_both)}")
    print(f"[AI ONLY] => {len(pmid_only_llm)}")
    print(f"[MANUAL ONLY] => {len(pmid_only_manual)}")
    print(f"[NEITHER] => {len(pmid_neither)}")
    print("-"*45)

    # create supp list
    print(f"[LLM] => {len(pmid_to_factors_llm)}")
    print(f"[MANUAL] => {len(pmid_to_factor_verified)}")
    print(f"[TOTAL] => {len(pmid_to_factors)}")
    supp_file = open("log/thrombose_supplemental.csv", "w")
    supp_file.write("PMID\n")
    for pmid in pmid_to_factors:
        if pmid not in pmid_to_factor_verified and pmid not in pmid_to_factors_llm:
            supp_file.write(f"{pmid}\n")
    supp_file.close()

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
        pmid_associated_to_factor_llm_approved = []
        pmid_associated_to_factor_verified = []
        for pmid in pmid_to_factors:
            if factor in pmid_to_factors[pmid]:
                pmid_associated_to_factor.append(pmid)

                # approved by llm
                if pmid in pmid_to_factors_llm:
                    if factor in pmid_to_factors_llm[pmid]:
                        if pmid not in pmid_associated_to_factor_llm_approved:
                            pmid_associated_to_factor_llm_approved.append(pmid)

                # manually verified
                if pmid in pmid_to_factor_verified:
                    if factor in pmid_to_factor_verified[pmid]:
                        if pmid not in pmid_associated_to_factor_verified:
                            pmid_associated_to_factor_verified.append(pmid)

        # fill status
        for status in status_list:
            status_line = ""
            for pmid in pmid_associated_to_factor:
                if pmid_to_status[pmid] == status:

                    # mark approved pmid
                    if (pmid in pmid_associated_to_factor_llm_approved and pmid in pmid_to_factor_verified):
                        pmid = f"[X][V]{pmid}"
                    elif pmid in pmid_associated_to_factor_llm_approved:
                        pmid = f"[X]{pmid}"
                    elif pmid in pmid_to_factor_verified:
                        pmid = f"[V]{pmid}"

                    status_line += f"{pmid} - "
            status_line = status_line[:-2]
            vector[status] = status_line

        matrix.append(vector)

    df = pd.DataFrame.from_dict(matrix)
    df.to_csv("tables/thrombose_table.csv", index=False)
    df.index = df["Factor"]
    df = df.drop(columns=["Factor"])
    df.to_html("tables/thrombose_table.tmp")

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

    table_data = open("tables/thrombose_table.html", "w")
    table_data.write(style)
    tmp_table = open("tables/thrombose_table.tmp", "r")
    for line in tmp_table:
        if re.search("^<table ", line):
            line = "<table id='machin'>\n"

        # catch factor

        elif re.search("[0-9]{8}", line):
            for elt in re.findall("([0-9]{8})", line):

                # check if factor picked by llm analysis

                line = line.replace(
                    elt, f"<a href='https://pubmed.ncbi.nlm.nih.gov/{elt}/'>{elt}</a>"
                )
        table_data.write(line)
    table_data.close()


if __name__ == "__main__":

    # get_thrombose_article()
    # pmid_to_status = assign_article_type()
    # pmid_to_factor_list = assign_factors()
    # print(pmid_to_factor_list)

    craft_table()
