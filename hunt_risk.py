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
