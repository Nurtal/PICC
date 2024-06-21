import pandas as pd
from langchain_community.llms import Ollama
from progress.bar import FillingCirclesBar
import pickle
import re


def run():
    """ """

    # get pmid to risk
    pmid_to_risk_manual = get_pmid_to_risk_manual()

    # get pmid to text
    pmid_to_text = get_pmid_to_text()

    # get pmid to risk - AI driven
    pmid_to_risk_ai = get_pmid_to_risk_ia(pmid_to_text, pmid_to_risk_manual)

    # save comparison file
    merge_observation(pmid_to_risk_manual, pmid_to_risk_ai)




def get_pmid_to_risk_manual():
    """ """

    # load data
    df = pd.read_csv('thrombose_filled.csv')
    df = df.fillna('tardis')
    df = df[df['Unnamed: 47'] == 'tardis']
    df = df.drop(columns=['Unnamed: 47'])
    df['PMID'] = df['PMID'].astype(str)
    risk_list = list(df.columns)

    # extract data
    pmid_to_risk = {}
    for index, row in df.iterrows():
        pmid = row['PMID']
        pmid_to_risk[pmid] = []
        for risk in risk_list:
            if str(row[risk]) == '1':
                pmid_to_risk[pmid].append(risk)

    # return data
    return pmid_to_risk


def get_pmid_to_text():
    """ """

    # load data
    df = pd.read_parquet("../data/thrombose_article.parquet") 

    # extract data
    pmid_to_text = {}
    for index, row in df.iterrows():
        pmid = row['PMID']
        text = f"{row['TITLE']}. {row['ABSTRACT']}".replace("  ", " ")
        pmid_to_text[pmid] = text

    # return data
    return pmid_to_text

        

def get_pmid_to_risk_ia(pmid_to_text, pmid_to_risk_m):
    """ """

    # init llm
    llm = Ollama(model="llama3:70b")

    # loop over data
    pmid_to_answer = {}
    pmid_to_risk_ai = {}
    bar = FillingCirclesBar("[EXTRACTION WITH LLM]", max=len(pmid_to_text))
    for pmid in pmid_to_text:
        text = pmid_to_text[pmid]
        risk_list = pmid_to_risk_m[pmid]
        pmid_to_risk_ai[pmid] = []
        pmid_to_answer[pmid] = {}

        for risk in risk_list:

            # craft prompt
            prompt = f"""

            [INSTRUCTION] : Determine if {risk} is a risk factor for picc-related thrombosis complications using only the following text. Answer only by yes / no

            [SENTENCE] : {text}

            [ANSWER]:
            """

            # run llama
            x = llm.invoke(prompt)

            # process answer
            if re.search('yes', x.lower()):
                pmid_to_risk_ai[pmid].append(risk) 
            
            # save results
            pmid_to_answer[pmid][risk] = x
            
    # Save log
    with open('llm_answer.pickle', 'wb') as handle:
        pickle.dump(pmid_to_risk_ai, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # return results
    return pmid_to_risk_ai

    


def merge_observation(pmid_to_risk_m, pmid_to_risk_ai):
    """ """

    # extract risk factors
    risk_list = []
    for pmid in pmid_to_risk_m:
        for risk in pmid_to_risk_m[pmid]:
            if risk not in risk_list:
                risk_list.append(risk)
    for pmid in pmid_to_risk_ai:
        for risk in pmid_to_risk_ai[pmid]:
            if risk not in risk_list:
                risk_list.append(risk)

    # init results
    output_data = open('comparison.csv', 'w')

    # write header
    header = 'PMID,'
    for x in risk_list:
        header+=str(x)+','
    header = header[:-1]
    output_data.write(f"{header}\n")
    for pmid in pmid_to_risk_m:
        line = f"{pmid},"
        for risk in risk_list:
            if risk in pmid_to_risk_m[pmid]:
                line += "[M]"
            if risk in pmid_to_risk_ai[pmid]:
                line += "[A]"
            line+=","
        line = line[:-1]
        output_data.write(f"{line}\n")
    
    # close file
    output_data.close()

    
    




if __name__ == "__main__":

    # get_pmid_to_risk_manual()
    # get_pmid_to_text()

    # pmid_to_risk_1 = {'745':['tociluzimab'], '888':['machin', 'chose']}
    # pmid_to_risk_2 = {'745' : [], '888':['machin']}

    # machin = get_pmid_to_risk_ia(pmid_to_text, pmid_to_risk)
    # print(machin)

    # merge_observation(pmid_to_risk_1, pmid_to_risk_2)

    run()
