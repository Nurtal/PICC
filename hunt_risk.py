import re

def scan_risk(target:str, text:str)->bool:
    """Scan risk for a target within a text.

    Args:
        target (str) : the potential risk
        test (str) : the context to scan
    
    Return:
        (bool) : True if target is a potential risk

    N.B : May be add a step 4 with a LLM that answer the question
    """

    # parameters
    target_can_be_a_risk = False
    risk_marker_list = ['risk', 'factor', 'impact', 'associate', 'drive', 'correlation', 'correlate', 'more', 'high', 'low', 'less', 'increase', 'decrease']
    risk_pattern = '|'.join(re.escape(term) for term in risk_marker_list)

    # step 1 - preprocess target and text
    target = target.lower()
    text = text.lower()
    sentence_list = text.split('. ')
    for sentence in sentence_list:

        # step 2 - check if target is present
        if(re.search(target, sentence)):

            # step 3 - Look for risk marker in the sentence
            if(re.search(risk_pattern, sentence)):

                # assign potential risk
                target_can_be_a_risk = True

    # return scan result
    return target_can_be_a_risk

def assgin_risk_factor(pmid_to_factors:dict, pmid_to_text:dict, factor_to_target:dict) -> dict:
    """Generate the use of scan_risk function for multiple pmid and facors"""
    
    # init data
    pmid_to_risk_factor = {}

    # scan
    for pmid in pmid_to_factors:
        factor_to_keep = []
        text = pmid_to_text[pmid]
        for factor in pmid_to_factors[pmid]:
            target_list = factor_to_target[factor]
            for target in target_list:
                if(factor not in factor_to_keep):
                    if(scan_risk(target, text)):
                        factor_to_keep.append(factor)
    pmid_to_risk_factor[pmid] = factor_to_keep
    
    # return data
    return pmid_to_risk_factor 





def get_pmid_to_text(data_file:str) -> dict:
    """Load pmid and its associate text (has the concatenation of title and abstract) into a dictionnary and return it """


    # load data
    df = pd.read_parquet(data_file)
    pmid_to_text = {}
    for index, row in df.iterrows():
        pmid = row["PMID"]
        title = row["TITLE"]
        abstract = row["ABSTRACT"]
        text = f"{title.lower()}. {abstract.lower()}".replace('.. ', '. ').replace('  ', ' ')
        pmid_to_text[pmid] = text

    # return data
    return pmid_to_text        


if __name__ == "__main__":

    # test parameters
    target_list = ['Age', 'old', 'older', 'young', 'elderly']
    target = 'lumens'
    abstract ="Conclusions: Immunosuppression and 3 PICC lumens were associated with increased risk of CLABSI. Power-injectable PICCs were associated with increased risk of CLABSI and VT formation. Postplacement adjustment of PICCs was not associated with increased risk of CLABSI or VT."
    test_abstract = "My cat is red. color have no impact on how stupid it is. Studies show that age is a risk factor for being stupid"

    if(scan_risk(target, abstract)):
        print(f"{target} can be a risk !")
    else:
        print(f"{target} doesn't seems to be a risk")
