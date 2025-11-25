from neo4j import GraphDatabase
from pandas import read_csv

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
CSV_FILE_PATH = '../movies_likes.csv'

driver = None
session = None
csv_file = None


# --- Initialize Resources ---
driver = GraphDatabase.driver(URI, auth=AUTH)
df = read_csv(CSV_FILE_PATH)

def populate_neo4j():
    #clearing the database
    driver.execute_query("MATCH (n) DETACH DELETE n", database_="neo4j")

    print("Populating data...")
    for _, row in df.iterrows():
        username = row["username"]
        movie_ids = row["movie_ids"].split(',')

        summary = driver.execute_query(
            """
            CREATE (u:User {username: $username} )
            WITH u
            UNWIND $movie_ids AS movieId
            MERGE (m:Movie {_id: movieId})
            CREATE (u)-[:LIKES]->(m)
            """, username=username, movie_ids=movie_ids, database_="neo4j"
        ).summary
        print("Created {nodes_created} nodes in {time} ms.".format(
            nodes_created=summary.counters.nodes_created,
            time=summary.result_available_after
        ))
    print("Data population complete.")

if __name__ == "__main__":
    populate_neo4j()
