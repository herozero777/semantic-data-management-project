# This is a sample Python script.
import pandas as pd
import json
from datetime import datetime
from os.path import join
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

path_to_data = join("data", "news_data.json")
output_path_n_filename = join("data", "news_flattened.csv")

with open(path_to_data, "r") as file_handle:
    person_dict = json.load(file_handle)

df = pd.DataFrame()
for mongodb_document in person_dict:
    news_fetch_date: str = mongodb_document["date"]["$date"]["$numberLong"]
    news_fetch_date: int = int(news_fetch_date)
    news_fetch_date: datetime = datetime.fromtimestamp( news_fetch_date / 1000 )
    for data in mongodb_document["data"]:
        company_name = data["company_name"]
        for news in data["news"]:
            news_title = news["title"]
            news_published_et = news["published_time"]
            news_source = news["source"]
            news_subjectivity = news["subjectivity"]
            news_polarity = news["polarity"]
            news_sentiment = news["sentiment"]

            row = pd.DataFrame([[news_fetch_date, company_name, news_title, news_published_et,
                                news_source, news_subjectivity, news_polarity, news_sentiment]],
                                columns=["news_fetch_date", "company_name", "news_title", "news_published_et",
                                "news_source", "news_subjectivity", "news_polarity", "news_sentiment"])

            df = pd.concat((df, row))


print(f"News JSON falttened, now saving !!!")
# Create news_id
df = df.reset_index(drop=True)
df = df.reset_index(drop=False)
df = df.rename(columns={"index": "news_id"})
df.to_csv(output_path_n_filename, index=False)


# Depricated
# %load_ext autotime

# import sys
# sys.path.append("../../..")
# from gastrodon import RemoteEndpoint,QName,ttl,URIRef,inline
# import pandas as pd
# pd.options.display.width=120
# pd.options.display.max_colwidth=100
#
#
# prefixes = inline("""
#     @prefix : <http://dbpedia.org/resource/> .
#     @prefix on: <http://dbpedia.org/ontology/> .
#     @prefix pr: <http://dbpedia.org/property/> .
# """).graph
#
# endpoint = RemoteEndpoint(
#     # "http://dbpedia.org/sparql/"
#     "http://localhost:7200/repositories/lab3"
#     ,prefixes=prefixes
#     ,default_graph="http://localhost:7200/lab3/"
#     ,base_uri="http://www.upc.abc/#"
# )

