from langchain_community.llms import Ollama
import pandas as pd
from progress.bar import FillingCirclesBar
import re


def run(data_file, output_file, log_file, risk):
    """ """

    # parameters
    if risk == "thrombosis":
        factor_to_llm_friendly_name = {
            "Two or more lumens": "Two or more lumens",
            "Power injectable PICC": "Power injectable PICC",
            "Immunosuppression": "Immunosuppression",
            "ICU": "intensive care unit",
            "Chemotherapy": "Chemotherapy",
            "Diabete": "Diabete",
            "Cancer": "Cancer",
            "Gender": "Gender",
            "BMI": "BMI / obesity",
            "Smoking": "Smoking",
            "Age": "Age",
            "History Of Thrombosis": "History Of Thrombosis",
            "Large catheter": "Large catheter",
            "PICC insertion procedure": "PICC insertion procedure",
            "PICC indwelling time": "PICC indwelling time",
            "Number of comorbidities/HTA/Hyperglycemia": "Number of comorbidities / HTA / Hyperglycemia",
            "Performans score_performance status_karnofsky_ECOG_Less activity_bedridden > 72h": " low activity / bedridden",
            "Total parenteral nutrition": "parenteral nutrition",
            "Biology ( blood platelet level_white blood cell level_triglycerides_fibrinogen)": "blood platelet or white blood cell or triglycerides or fibrinogen",
            "Upper extremity damages": "Upper extremity damages",
            "Surgery_type of surgery": "surgery",
            "Transplantation": "transplantation",
            "High intrathoracic pressure": "High intrathoracic pressure",
            "PICC infection": "infection",
            "Menopause_endocrine therapy_hormone use": "Menopause / therapy_hormone use",
            "Erythropoiesis-stimulating agents": "Erythropoiesis-stimulating agents",
            "Slower blood flow velocity": "blood flow velocity",
            "Non-O blood type_ group B": "blood type",
            "Anticoagulants therapy": "anticoagulant",
            "Thrombophilia": "thrombophilia",
            "Others (COPD, IBD)": "COPD/IBD",
            "Arm_laterality _vein choice": "arm laterality / vein choice",
            "Hypertension": "hypertension",
            "Hospitalisation/ Medical department admission/ discharged to a skilled-nursing facility": "medical department admission",
            "Blood vessel irritating drug_vancomycin_amphotericine": "blood vessel irritating drug",
        }
    elif risk == "infection":
        factor_to_llm_friendly_name = {
            "Advanced Age": "Advanced age",
            "Chemotherapy": "Chemotherapy",
            "Two or more lumens": "Two or more lumens",
            "Antibiotic-coated catheters": "Antibiotic-coated catheters",
            "Previous PICC": "Previous PICC",
            "Antibiotic therapy": "Antibiotic therapy",
            "Parenteral nutrition": "Parenteral nutrition",
            "Materials of PICC": "Materials of PICC",
            "PICC dwell time": "PICC dwell time",
            "Medical department admission (including ICU)": "Medical department admission / ICU",
            "Acute myeloid leukemia": "Acute myeloid leukemia / leukemia",
            "Auto/allograft": "Auto/allograft",
            "Anticoagulants therapy": "Anticoagulants therapy / anti-coagulant therapy",
            "Immunosuppression": "Immunosuppression",
            "Hospital length of stay": "Hospital length of stay",
            "Seasonality (summer / warm)": "Seasonality (summer / warm)",
            "Operator experience": "Operator experience",
            "Postplacement": "Postplacement",
            "Fixation device": "Fixation device",
            "Catheter care delay": "Catheter care delay",
            "Power-injectable piccs": "Power-injectable piccs",
        }

    # load data
    df = pd.read_csv(data_file)
    answer_to_tag = {1: "Yes", 0: "No", -1: "IDK"}

    data = []
    log_data = []
    bar = FillingCirclesBar("[EXTRACTION WITH LLM]", max=df.shape[0])
    for index, row in df.iterrows():

        pmid = row["PMID"]
        factor = row["FACTOR"]
        text = row["TEXT"]

        answer = run_extraction(factor_to_llm_friendly_name[factor], text, risk)
        # answer = two_stage_extraction(factor_to_llm_friendly_name[factor], text, risk)
        tag = answer_to_tag[process_answer(answer)]
        data.append({"PMID": pmid, "FACTOR": factor, "RISK": tag, "TEXT": text})
        log_data.append(
            {
                "PMID": pmid,
                "FACTOR": factor,
                "RISK": tag,
                "TEXT": text,
                "LLM_OUTPUT": answer,
            }
        )

        bar.next()

    df = pd.DataFrame.from_dict(data)
    df.to_csv(output_file, index=False)
    df_log = pd.DataFrame.from_dict(log_data)
    df_log.to_csv(log_file, index=False)

    # extract pmid to factor
    pmid_to_factor = extract_pmid_to_factor(output_file)

    # return
    return pmid_to_factor


def run_extraction(factor, text, risk):
    """ """

    llm = Ollama(model="llama3:70b")

    if risk == "thrombosis":

        # V1
        prompt = f"""

        [INSTRUCTION] : Determine if {factor} is a risk factor for picc-related thrombosis complications using only the following text. Answer only by yes / no

        [SENTENCE] : {text}

        [ANSWER]:
        """

        # v2
        # prompt = f"""
        #
        # [INSTRUCTION] : Read the following abstract and answer 'yes' if {factor} is evaluated as a risk factor and if its associated to picc-related thrombosis complications. Answer only by yes / no
        #
        # [ABSTRACT] : {text}
        #
        # [ANSWER]:
        # """

    elif risk == "infection":
        prompt = f"""

        [INSTRUCTION] : Determine if {factor} is a risk factor for picc-related infection using only the following text. Answer only by yes / no

        [SENTENCE] : {text}

        [ANSWER]:
        """

        # v2
        # prompt = f"""
        #
        # [INSTRUCTION] : Read the following abstract and answer 'yes' if {factor} is evaluated as a risk factor and if its associated to picc-related infection. Answer only by yes / no
        #
        # [ABSTRACT] : {text}
        #
        # [ANSWER]:
        # """

    else:
        return "No Risk Specified"

    x = llm.invoke(prompt)
    return x


def process_answer(answer):
    """ """

    # parameter
    clean_answer = -1

    # step 1 - cacth short and simple answer
    if len(answer) <= 5:
        if re.search("yes", answer.lower()):
            clean_answer = 1
        elif re.search("no", answer.lower()):
            clean_answer = 0

    # step 2 - catch understable sentences
    else:
        answer = answer.replace("\n", ". ")
        sentence_list = answer.split(". ")
        for sentence in sentence_list:
            if re.search("answer", sentence.lower()):
                if re.search("yes", sentence.lower()):
                    clean_answer = 1
                elif re.search("no", sentence.lower()):
                    clean_answer = 0

            if len(sentence) <= 10:
                if re.search("yes", sentence.lower()):
                    clean_answer = 1
                elif re.search("no", sentence.lower()):
                    clean_answer = 0
            if len(sentence) > 10:
                short_sentence = sentence[0:9]
                if re.search("yes", short_sentence.lower()):
                    clean_answer = 1
                elif re.search("no", short_sentence.lower()):
                    clean_answer = 0

    # step 3 - scan begining of the sentence
    if len(answer) >= 20:
        short_answer = answer[0:15]
        if re.search("yes", short_answer.lower()):
            clean_answer = 1
        elif re.search("no", short_answer.lower()):
            clean_answer = 0

    return clean_answer


def pick_random_sample(result_file, output_file, sample_size):
    """ """

    # load data
    df = pd.read_csv(result_file)

    # just for throbosis - tmp stuff
    # df = df[df["FACTOR"].isin(["PICC infection"])]

    # pick random
    df = df.sample(n=sample_size)

    # save
    df.to_csv(output_file, index=False)


def extract_pmid_to_factor(llm_processed_file: str) -> dict:
    """Read datafile genareted by the LLM and keep only validated risk factors

    Args:
        - llm_processed_file (str) : path to the data file to parse
    Returns:
        - pmid_to_factor (dict) : pmid : [risk_factor]

    """

    # load data
    pmid_to_factor = {}
    df = pd.read_csv(llm_processed_file)

    # keep only the identified factors
    df = df[df["RISK"] == "Yes"]
    for index, row in df.iterrows():
        pmid = str(row["PMID"])
        factor = row["FACTOR"]
        if pmid not in pmid_to_factor:
            pmid_to_factor[pmid] = [factor]
        else:
            pmid_to_factor[pmid].append(factor)

    # return pmid to factor
    return pmid_to_factor


def two_stage_extraction(factor: str, text: str, complication: str) -> str:
    """Two stage extraction model

    Args:
        - factor (str) : the factor to evaluate
        - text (str) : the text tin inject in the prompt
        - complication (str) : either infection or thrombosis

    Returns:
        (str) : answer of the extraction model, supposed to be either Yes or No

    """

    llm = Ollama(model="llama3:70b")

    # fisrt stage of the model, check if the evaluate factor is a factor, a complication or a condition
    prompt = f"""
    [INSTRUCTION] : In the following abstract, determine if {factor} is a risk factor (A), a complication (B) or a condition (C). Answer only by A / B / C

    [ABSTRACT] : {text}

    [ANSWER]:
    """
    x = llm.invoke(prompt)

    # decide if we triger the second stage
    if x not in ["B", "C"]:

        # second stage of the model, check if factor is associated with complication
        answer = run_extraction(factor, text, complication)
    else:
        answer = "No"

    return answer


if __name__ == "__main__":

    # run_extraction(
    #     "chemotherapy",
    #     "PICCs were associated with significant complications in hospitalized patients who had solid malignancies and were often used for reasons other than chemotherapy",
    # )

    # parameters
    # data_file = "data/thrombose_test_risk_abstract.csv"
    # output_file = "data/thrombose_extracted_risk_with_llama3.csv"
    output_file = "data/thrombose_llama3_extracted_risk_abstract.csv"
    output_file = "data/infection_extracted_risk_with_llama3.csv"
    # output_file = "data/infection_extracted_risk_with_llama3.csv"
    # log_file = "log/thrombose_risk_extract_llama3_abstract.log"

    # run(data_file, output_file, log_file)
    pick_random_sample(output_file, "data/test_eval.csv", 5)

    # pmid_to_factor = extract_pmid_to_factor(output_file)
    abstract = "PICCs were associated with significant complications in hospitalized patients who had solid malignancies and were often used for reasons other than chemotherapy"

    two_stage_extraction("Cancer", abstract, "thrombosis")
