import redis
import numpy as np
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from redis.commands.search.field import VectorField
from redis.commands.search.indexDefinition import IndexDefinition

INDEX_NAME = "movie_plot_index"

def populate_redis():
    print("Initializing clients and model...")
    redis_client = redis.Redis(decode_responses=False)
    mongo_client = MongoClient()
    db = mongo_client["moviesdb"]
    model = SentenceTransformer("avsolatorio/GIST-small-Embedding-v0")
    print("Initialization complete")

    schema = (
        VectorField(
            "plot_embedding", "HNSW", {
                "TYPE": "FLOAT32",
                "DIM": 384,
                "DISTANCE_METRIC": "COSINE"
            }
        ),
    )
    
    print(f"Setting up Redis index '{INDEX_NAME}'...")
    if INDEX_NAME in redis_client.execute_command("FT._LIST"):
        redis_client.ft(INDEX_NAME).dropindex()
        print("Dropped existing index.")
    else:
        print("No existing index to drop, proceeding to create a new one.")
        
    redis_client.ft(INDEX_NAME).create_index(
        fields=schema, 
        definition=IndexDefinition(prefix=["movie:"])
    )
    print("Index created successfully")


    print(f"Fetching movies with more than 500 votes from MongoDB...")
    movies_cursor = db["movies"].find({"vote_count": {"$gt": 500}})
    
    pipe = redis_client.pipeline()
    
    movie_count = 0
    for movie in movies_cursor:
        plot = movie.get("plot") or movie.get("overview")
        movie_id = movie.get("_id")

        if not movie_id or not plot:
            continue

        embedding = model.encode(plot, normalize_embeddings=True)
        
        embedding_bytes = np.array(embedding).astype(np.float32).tobytes()

        redis_key = f"movie:{movie_id}"
        pipe.hset(redis_key, "plot_embedding", embedding_bytes)
        
        movie_count += 1

    print(f"Storing {movie_count} movie embeddings in Redis...")
    pipe.execute()
    
    print("Completed")

if __name__ == "__main__":
    populate_redis()
