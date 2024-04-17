import pandas as pd


def check_factors(pmid_to_factor, check_data_file, log_file):
    """ """

    # parameter
    factor_to_pmid = {}
    log_data = open(log_file, "w", encoding='utf-8')

    # load check table
    df = pd.read_csv(check_data_file)

    # check that pmid are correctly assigned
    for pmid in pmid_to_factor:
        for factor in pmid_to_factor[pmid]:
            if factor not in list(df["FACTOR"]):
                log_data.write(f"[ERROR][KEY] Factor {factor} not found in table\n")
            else:
                for index, row in df.iterrows():

                    table_factor = row["FACTOR"]
                    pmid_list = row["PMID_LIST"].split(";")
                    factor_to_pmid[table_factor] = pmid_list

                    if factor == table_factor:
                        if pmid in pmid_list:
                            log_data.write(
                                f"[OK] Found factor {factor} for pmid {pmid}\n"
                            )
                        else:
                            log_data.write(
                                f"[ERROR] Factor {factor} not found for pmid {pmid}\n"
                            )
    # look for missing pmid
    for factor in factor_to_pmid:
        for pmid in factor_to_pmid[factor]:
            if pmid not in list(pmid_to_factor.keys()):
                log_data.write(f"[MISSING] {pmid} is missing for factor {factor}\n")

    # close log
    log_data.close()


def add_controled_factors(pmid_to_factor:dict, check_data_file:str, log_file:str):
    """ """

    # parameter
    factor_to_pmid = {}
    log_data = open(log_file, "w", encoding='utf-8')

    # load check table
    df = pd.read_csv(check_data_file)

    # get factor to pmid
    for index, row in df.iterrows():
        table_factor = row["FACTOR"]
        pmid_list = row["PMID_LIST"].split(";")
        factor_to_pmid[table_factor] = pmid_list

    # get authorized factors list
    authorized_factor_list = []
    for pmid in pmid_to_factor:
        for f in pmid_to_factor[pmid]:
            if(f not in authorized_factor_list):
                authorized_factor_list.append(f)
    
    # look for missing pmid
    for factor in factor_to_pmid:
        if(factor in authorized_factor_list):
        
            for pmid in factor_to_pmid[factor]:
                if pmid not in list(pmid_to_factor.keys()):
                    pmid_to_factor[pmid] = [factor]
                    log_data.write(f"[ADD] {pmid} associated to {factor}\n")
                else:
                    pmid_to_factor[pmid].append(factor)
                    log_data.write(f"[ADD] {factor} in factor list of {pmid}\n")
        else:
            log_data.write(f"[WARNING] {factor} not found in original data\n")
            
    # close log
    log_data.close()
    
    # return updated data
    return pmid_to_factor


if __name__ == "__main__":

    # parameters
    pmid_to_factor = {
        "24612469": ["Number of lumens", "couleur du chat"],
        "123": ["Number of lumens"],
    }
    check_data_file = "data/check_table3_fdr.csv"
    log_file = "log/check.log"

    check_factors(pmid_to_factor, check_data_file, log_file)
