import re
import pandas as pd
import os


def scan_risk(target: str, text: str, log_file: str) -> bool:
    """Scan risk for a target within a text.

    Args:
        target (str) : the potential risk
        test (str) : the context to scan
        log_file (str) : path to log file, should already exist

    Return:
        (bool) : True if target is a potential risk

    N.B : May be add a step 4 with a LLM that answer the question
    """

    # parameters
    target_can_be_a_risk = False
    risk_marker_list = [
        "risk",
        "factor",
        "impact",
        "associate",
        "drive",
        "correlation",
        "correlate",
        "more",
        "high",
        "low",
        "less",
        "increase",
        "decrease",
        "group",
    ]
    risk_pattern = "|".join(re.escape(term) for term in risk_marker_list)

    # step 0 - open log file
    log_data = open(log_file, "a", encoding="utf-8")

    # step 1 - preprocess target and text
    target = target.lower()
    text = text.lower()
    sentence_list = text.split(". ")
    for sentence in sentence_list:

        # step 2 - check if target is present
        if re.search(target, sentence):
            log_data.write(f"[TARGET] => found {target} in {sentence}\n")

            # step 3 - Look for risk marker in the sentence
            for risk in risk_marker_list:
                if re.search(risk, sentence):
                    log_data.write(f"[RISK] => found {risk} in {sentence}\n")

                    # assign potential risk
                    target_can_be_a_risk = True

    # return scan result
    log_data.close()
    return target_can_be_a_risk


def assgin_risk_factor(
    pmid_to_factors: dict, pmid_to_text: dict, factor_to_target: dict
) -> dict:
    """Generate the use of scan_risk function for multiple pmid and facors"""

    # init data
    pmid_to_risk_factor = {}
    log_file = "log/scan.log"
    log_data = open(log_file, "w", encoding="utf-8")
    log_data.close()

    f_list1 = []
    for pmid in pmid_to_factors:
        for f in pmid_to_factors[pmid]:
            if f not in f_list1:
                f_list1.append(f)
    f_list2 = list(factor_to_target.keys())
    for f in f_list1:
        if f not in f_list2:
            print(f" ==> {f}")

    # scan
    for pmid in pmid_to_factors:
        log_data = open(log_file, "a")
        log_data.write(f"# {pmid} ---------\n")
        log_data.close()
        factor_to_keep = []
        text = pmid_to_text[pmid]
        for factor in pmid_to_factors[pmid]:
            log_data = open(log_file, "a", encoding="utf-8")
            log_data.write(f"## -> Scan Factor {factor}\n")
            log_data.close()
            target_list = factor_to_target[factor]
            for target in target_list:
                if factor not in factor_to_keep:
                    if scan_risk(target, text, log_file):
                        factor_to_keep.append(factor)
        pmid_to_risk_factor[pmid] = factor_to_keep

    # return data
    return pmid_to_risk_factor


def get_pmid_to_text(data_file: str) -> dict:
    """Load pmid and its associate text (has the concatenation of title and abstract) into a dictionnary and return it"""

    # load data
    df = pd.read_parquet(data_file)
    pmid_to_text = {}
    for index, row in df.iterrows():
        pmid = row["PMID"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]
        text = f"{title.lower()}. {abstract.lower()}".replace(".. ", ". ").replace(
            "  ", " "
        )
        pmid_to_text[pmid] = text

    # return data
    return pmid_to_text


if __name__ == "__main__":

    # test parameters
    target_list = ["Age", "old", "older", "young", "elderly"]
    target = "lumens"
    abstract = "Conclusions: Immunosuppression and 3 PICC lumens were associated with increased risk of CLABSI. Power-injectable PICCs were associated with increased risk of CLABSI and VT formation. Postplacement adjustment of PICCs was not associated with increased risk of CLABSI or VT."
    test_abstract = "My cat is red. color have no impact on how stupid it is. Studies show that age is a risk factor for being stupid"

    # if scan_risk(target, abstract):
    #     print(f"{target} can be a risk !")
    # else:
    #     print(f"{target} doesn't seems to be a risk")

    pmid_to_text = get_pmid_to_text("data/test.parquet")
    pmid_to_factors = {
        "24440542": [
            "Advanced Age",
            "Two or more lumens",
            "Antibiotic therapy",
            "Parenteral nutrition",
            "PICC dwell time",
            "Medical department admission (including ICU)",
            "Hospital length of stay",
        ]
    }

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
    pmid_to_risk = assgin_risk_factor(pmid_to_factors, pmid_to_text, factor_to_target)
    print(pmid_to_risk)
