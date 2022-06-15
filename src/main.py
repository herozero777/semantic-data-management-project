from src.utils import Neo4jConnection
from datetime import datetime
import time
import pandas as pd
from os.path import join

input_file_path = join("data", "news_flattened.csv")


##################################
# Util functions
##################################
def clean_str(raw_str):
    """
    :param raw_str: String that contains double quotes (") and single quotes (') that we want to remove
    :return: raw_str
    """
    raw_str = raw_str.replace("\"", "_")
    raw_str = raw_str.replace("\'", "_")
    return raw_str


def delete_all_existing_nodes(conn: Neo4jConnection):
    print(f"Deleting all existing nodes and edges.")
    tic = time.perf_counter()
    query_delete_all_existing_nodes = '''
        MATCH (n) DETACH DELETE n
    '''
    conn.query(query_delete_all_existing_nodes, db='neo4j')
    toc = time.perf_counter()
    print(f"Total time: {toc - tic:0.4f} seconds\n")


# ---
def create_nodes(conn: Neo4jConnection, df: pd.DataFrame):
    print(f"Creating Nodes and Edges.")
    tic = time.perf_counter()
    for _, row in df.iterrows():
        news_et = datetime.strptime(row["news_published_et"], "%Y-%m-%dT%H:%M:%SZ")
        news_published_month = news_et.month
        news_published_year = news_et.year
        news_title = clean_str(row["news_title"])

        query = f"""
        MERGE (c:Company {{name: "{row["company_name"]}" }} )
        MERGE (n:News {{id: toInteger({row["news_id"]}), title: '{news_title}',
                        news_polarity: toFloat({row["news_polarity"]}),
                        news_subjectivity: toFloat({row["news_subjectivity"]}) }} )
        MERGE (s:Source {{name: "{row["news_source"]}" }} )
        MERGE (sen: Sentiment {{name: "{row["news_sentiment"]}" }} )
        MERGE (m:Month {{value: toInteger({news_published_month}) }})
        MERGE (y:Year {{value: toInteger({news_published_year}) }})
    
        MERGE (n)-[:HAS_COMPANY]->(c)
        MERGE (n)-[:HAS_SOURCE]->(s)
        MERGE (n)-[:HAS_SENTIMENT]->(sen)
        MERGE (n)-[:HAS_MONTH]->(m)
        MERGE (n)-[:HAS_YEAR]->(y)
        """

        conn.query(query, db='neo4j')
        toc = time.perf_counter()
        print(f"Total time: {toc-tic:0.4f} seconds\n")

# ---


##################################
# Main Program Run
##################################

if __name__ == '__main__':
    conn = Neo4jConnection(uri="bolt://localhost:7687", user="neo4j", pwd="sdm")

    df = pd.read_csv(input_file_path)
    df = df[:30]

    print(f"\n**** Starting neo4j database initialization ****\n")
    main_tic = time.perf_counter()

    delete_all_existing_nodes(conn)
    create_nodes(conn, df)

    main_toc = time.perf_counter()
    print(f"*** Initialization Complete. Total time: {main_toc - main_tic:0.4f} seconds ****")