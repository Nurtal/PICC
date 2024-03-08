import re
import polars as pl


def convert(input_file, output_file):
    """ """

    article_list = {}
    record_title = False
    record_abstract = False
    input_data = open(input_file)
    for line in input_data:
        line = line.rstrip()

        if re.search("^PMID-", line):
            pmid = line.split("- ")[-1]
            article_list[pmid] = {"PMID": pmid}

        if re.search("^DCOM-", line):
            date = line.split("- ")[-1][0:4]
            article_list[pmid]["YEAR"] = date
        if re.search("^DP ", line):
            date = line.split("- ")[-1][0:4]
            article_list[pmid]["YEAR"] = date

        # deal with title
        if re.search("^TI ", line):
            article_list[pmid]["TITLE"] = line + " "
            record_title = True

        if re.search("^ ", line) and record_title:
            article_list[pmid]["TITLE"] += line + " "

        if (
            re.search("^PG", line)
            and record_title
            or re.search("^LID ", line)
            and record_title
        ):
            record_title = False

        # deal with abstract
        if re.search("^AB ", line):
            article_list[pmid]["ABSTRACT"] = line + " "
            record_abstract = True

        if re.search("^ ", line) and record_abstract:
            article_list[pmid]["ABSTRACT"] += line + " "

        if (
            re.search("^CI", line)
            and record_abstract
            or re.search("^FAU", line)
            and record_abstract
            or re.search("^CN", line)
            and record_abstract
            or re.search("^LA", line)
            and record_abstract
        ):
            record_abstract = False

        if re.search("^LA ", line):
            language = line.split(" - ")[-1]
            if "LANGUAGE" not in list(article_list[pmid].keys()):
                article_list[pmid]["LANGUAGE"] = [language]
            else:
                article_list[pmid]["LANGUAGE"].append(language)

        if re.search("^MH ", line):
            mh = line.split(" - ")[-1]
            if "MH" not in list(article_list[pmid].keys()):
                article_list[pmid]["MH"] = [mh]
            else:
                article_list[pmid]["MH"].append(mh)

        if re.search("^OT ", line):
            ot = line.split(" - ")[-1]
            if "OT" not in list(article_list[pmid].keys()):
                article_list[pmid]["OT"] = [ot]
            else:
                article_list[pmid]["OT"].append(ot)

        if re.search("^PT ", line):
            pt = line.split(" - ")[-1]
            if "TYPE" not in list(article_list[pmid].keys()):
                article_list[pmid]["TYPE"] = [pt]
            else:
                article_list[pmid]["TYPE"].append(pt)
    input_data.close()

    # clean data
    data = []
    for article in article_list.values():

        # clean keys
        for k in ["MH", "OT", "LANGUAGE", "TYPE"]:
            if k not in article:
                article[k] = []
        for k in ["ABSTRACT", "TITLE"]:
            if k not in article:
                article[k] = ""

        # clean title
        try:
            article["TITLE"] = (
                article["TITLE"].replace("       ", " ").replace("TI  - ", "")
            )
        except:
            pass

        # clean abstract
        try:
            article["ABSTRACT"] = (
                article["ABSTRACT"].replace("       ", " ").replace("AB  - ", "")
            )
        except:
            pass
        data.append(article)

    # craft data file
    df = pl.DataFrame._from_dicts(data)
    df.write_parquet("pubmed.parquet")


if __name__ == "__main__":

    # parameters
    input_file = "pubmed.txt"
    output_file = "pubmed.parquet"

    convert(input_file, output_file)
