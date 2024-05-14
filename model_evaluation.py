import pandas as pd
from pandas.core.reshape.merge import restore_dropped_levels_multijoin
from sklearn.metrics import accuracy_score
from sklearn import metrics
import matplotlib.pyplot as plt
from sklearn import metrics
import seaborn as sns
import numpy as np


def run_evaluation(thrombose_file, infection_file, validation_file, result_file):
    """ """

    # init
    key_to_true = {}
    key_to_predicted = {}

    # load evaluation file
    df = pd.read_csv(validation_file)
    for index, row in df.iterrows():
        pmid = row["PMID"]
        complication = row["COMPLICATION"]
        factor = row["FACTOR"]
        risk = row["MANUAL"]
        if risk == "Yes":
            risk = 1
        if risk == "No":
            risk = 0
        if risk == "IDK":
            risk = 0
        key_to_true[f"{pmid}_{complication}_{factor}"] = risk

    # load thrombose extracted risk
    df_thr = pd.read_csv(thrombose_file)
    for index, row in df_thr.iterrows():
        pmid = row["PMID"]
        complication = "THROMBOSIS"
        factor = row["FACTOR"]
        risk = row["RISK"]
        if risk == "Yes":
            risk = 1
        if risk == "No":
            risk = 0
        if risk == "IDK":
            risk = 0
        key = f"{pmid}_{complication}_{factor}"
        if key in key_to_true:
            key_to_predicted[key] = risk

    # load infection extracted risk
    df_inf = pd.read_csv(infection_file)
    for index, row in df_inf.iterrows():
        pmid = row["PMID"]
        complication = "INFECTION"
        factor = row["FACTOR"]
        risk = row["RISK"]
        if risk == "Yes":
            risk = 1
        if risk == "No":
            risk = 0
        if risk == "IDK":
            risk = 0
        key = f"{pmid}_{complication}_{factor}"
        if key in key_to_true:
            key_to_predicted[key] = risk

    # compute Key intersection
    key_to_true_clean = {}
    ok_missed_keys = []
    bad_missed_keys = []
    for k in key_to_true:
        if k not in key_to_predicted:

            if key_to_true[k] == 0:
                ok_missed_keys.append(k)
            else:
                bad_missed_keys.append(k)
        else:
            key_to_true_clean[k] = key_to_true[k]
    key_to_true = key_to_true_clean

    # compute acc & auc
    true = []
    prediction = []
    for k in key_to_true:
        true.append(key_to_true[k])
        prediction.append(key_to_predicted[k])
    acc = accuracy_score(true, prediction)
    fpr, tpr, thresholds = metrics.roc_curve(true, prediction, pos_label=1)
    auc = metrics.auc(fpr, tpr)

    
    plt.plot(fpr,tpr,label="LLM, auc="+str(auc))
    plt.legend(loc=4)
    plt.savefig('images/roc_curve.png')
    plt.close()

    
    # generate confusion matrix
    cnf_matrix = metrics.confusion_matrix(true, prediction)
    class_names=[1,0] # name  of classes
    fig, ax = plt.subplots()
    tick_marks = np.arange(len(class_names))
    plt.xticks(tick_marks, class_names)
    plt.yticks(tick_marks, class_names)
    sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
    ax.xaxis.set_label_position("top")
    plt.tight_layout()
    plt.title('Confusion matrix', y=1.1)
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')
    plt.savefig('images/confusion_matrix.png')
    plt.close()

    # save result
    result_data = open(result_file, "w")
    result_data.write(f"[ACC] => {acc}\n")
    result_data.write(f"[AUC] => {auc}\n")
    for k in ok_missed_keys:
        result_data.write(f"[MISSING KEY][OK] => {k}\n")
    for k in bad_missed_keys:
        result_data.write(f"[MISSING KEY][ERROR] => {k}\n")
    for k in key_to_true:
        y = key_to_true[k]
        x = key_to_predicted[k]
        if x != y:
            result_data.write(f"[MISS CLASSIFIED] -> {k} => {x} instead of {y} \n")
    result_data.close()


if __name__ == "__main__":

    # parameters
    thrombose_file = "data/thrombose_extracted_risk_with_llama3.csv"
    infection_file = "data/infection_extracted_risk_with_llama3.csv"
    validation_file = "data/llm_validation_big.csv"
    result_file = "log/model_eval.log"

    run_evaluation(thrombose_file, infection_file, validation_file, result_file)
