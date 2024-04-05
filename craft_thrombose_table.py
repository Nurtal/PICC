import pandas as pd
import re


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
    """Load pmid from infection file and assign factors to each of them"""

    # load data
    df = pd.read_parquet("data/thrombose_article.parquet")

    factor_list = [
        "Number of lumens_two or more lumens",
        "Power injectable PICC",
        "Immunosuppression",
        "Intensive care unit",
        "Chemotherapy_chemotherapy history_Fluoropyrimidine_etoposide_carboplatin_docetaxel",
        "Diabetes",
        "Cancer_carcinoma_metastasis_Hematologic malignancy_leukemia_lymphoma_Recent neoplasia_active neoplasia",
        "Sex_male",
        "BMI_height_obesity",
        "Smoking_History of smoking",
        "Age",
        "Thrombosis_History of thrombosis_past history of VTE",
        "Large catheter_large gauge catheter_catheter to vein ratio",
        "PICC insertion procedure_position_more than one attempt for PICC insertion_operator experience_cutting or trimming the tip before insertion_Blood vessel traumatism_vein depth",
        "PICC indwelling time_catheter retention time",
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


if __name__ == "__main__":

    get_thrombose_article()
    pmid_to_status = assign_article_type()
