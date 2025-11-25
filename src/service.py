from bson import json_util
from typing import Dict, List, Tuple
import datetime
import time
import numpy as np
from database import Database

from redis.commands.search.query import Query


DB = Database()


# DO NOT MODIFY THIS FUNCTION
def measure(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        try:
            return func(*args, **kwargs)
        finally:
            end = time.time()
            store_metric(func.__name__, end - start)

    return wrapper


@measure
def search_movie(text):
    """
    Search movies by title and sort the results using a custom score calculated as `textScore * popularity`.
    Also return facets for field `genre`, `releaseYear` and `votes`.

    Hint: check MongoDB's $facet stage
    """
    pipeline = [
        {"$match": {"$text": {"$search": text}}},
        {"$addFields": {
            "score": {"$multiply": [{"$meta": "textScore"}, "$popularity"]},
        }},
        {"$facet": {
            "searchResults": [
                {"$sort": {"score": -1}},
                {"$limit": 25},
                {"$project": {"_id": 1, "poster_path": 1, "release_date": 1, "title": 1, "vote_average": 1, "vote_count": 1, "score": 1}}
            ],
            "genreFacet": [
                {"$unwind": "$genres"},
                {"$match": {"genres": {"$ne": None, "$ne": ""}}},
                {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
            ],
            "releaseYearFacet": [
                {"$match": {"release_date": {"$ne": None}}},
                {"$group": {"_id": {"$year": "$release_date"}, "count": {"$sum": 1}}},
                {"$sort": {"_id": -1}}
            ],
            "votesFacet": [
                {"$group": {"_id": "$vote_count", "count": {"$sum": 1}}},
                {"$sort": {"_id": -1}}
            ]
        }}
    ]
    results = list(DB.mongo_db().movies.aggregate(pipeline))
    if results:
        return results[0]
    else:
        return None


@measure
def get_top_rated_movies():
    """
    Return top rated 25 movies with more than 5k votes
    """
    return DB.mongo_db().movies.find(
        {
            "vote_count": {
                "$gt": 5000
            }
        },
        {
            "_id": 1,
            "poster_path": 1,
            "release_date": 1,
            "title": 1,
            "vote_average": 1,
            "vote_count": 1
        }
    ).sort("vote_average", -1).limit(25)


@measure
def get_recent_released_movies():
    """
    Return recently released movies that at least are reviewed by 50 users
    """
    return DB.mongo_db().movies.find(
        {
            "vote_count": {
                "$gte": 50
            }
        },
        {
            "_id": 1,
            "poster_path": 1,
            "release_date": 1,
            "title": 1,
            "vote_average": 1,
            "vote_count": 1
        }
    ).sort("release_date", -1).limit(25)


@measure
def get_movie_details(movie_id):
    """
    Return detailed information for the specified movie_id
    """
    results =  DB.mongo_db().movies.find_one(
        {
            "_id": int(movie_id)
        },
        {
            "_id": 1,
            "genres": 1,
            "overview": 1,
            "poster_path": 1,
            "release_date": 1,
            "tagline": 1,
            "title": 1,
            "vote_average": 1,
            "vote_count": 1
        }
    )
    results["release_date"] = results["release_date"].strftime("%B %d, %Y") if results.get("release_date") else ""
    return results


@measure
def get_same_genres_movies(movie_id, genres):
    """
    Return a list of movies that match at least one of the provided genres.

    Movies need to be sorted by the number genres that match in descending order
    (a movie matching two genres will appear before a movie only matching one). When
    several movies match with the same number of genres, movies with greater rating must
    appear first.

    Discard movies with votes by less than 500 users. Limit to 8 results.
    """
    pipeline = [
        {"$match": {
            "_id": {"$ne": int(movie_id)},
            "genres": {"$in": genres},
            "vote_count": {"$gte": 500}
        }},
        {"$addFields": {
            "match_count": {"$size": {"$setIntersection": ["$genres", genres]}}
        }},
        {"$sort": {"match_count": -1, "vote_average": -1}},
        {"$project": {
            "_id": 1,
            "genres": 1,
            "poster_path": 1,
            "release_date": 1,
            "title": 1,
            "vote_average": 1,
            "vote_count": 1
        }},
        {"$limit": 8}
    ]
    return DB.mongo_db().movies.aggregate(pipeline)


@measure
def get_similar_movies(movie_id):
    """
    Return a list of movies with a similar plot as the given movie_id.
    Movies need to be sorted by the popularity instead of proximity score.
    """

    target_embedding_bytes = DB.redis_db().hget(f"movie:{movie_id}", "plot_embedding")

    # Some movies might not have an embedding (less than 500 votes)
    if not target_embedding_bytes:
        return []

    results = DB.redis_db().ft("movie_plot_index").search(
        Query("*=>[KNN 20 @plot_embedding $query_vector AS vector_score]").return_fields("id").dialect(2),
        {"query_vector": target_embedding_bytes}
    ).docs

    similar_movie_ids = []
    for doc in results:
        retrieved_id = int(doc.id.replace("movie:", ""))
        if retrieved_id != int(movie_id):
            similar_movie_ids.append(retrieved_id)
    
    if not similar_movie_ids:
        return []

    return DB.mongo_db().movies.find(
        {"_id": {"$in": similar_movie_ids}},
        {"_id": 1,
         "poster_path": 1,
         "release_date": 1,
         "title": 1,
         "vote_average": 1,
         "vote_count": 1}
    ).sort("popularity", -1)


@measure
def get_movie_likes(username, movie_id):
    """
    Returns a list of usernames of users who also like the specified movie_id
    """
    query = f"""
        MATCH (m:Movie {{_id: '{movie_id}'}})<-[:LIKES]-(u:User)
        WHERE u.username <> '{username}'
        RETURN u.username AS username
    """
    records, _, _ = DB.neo4j_driver.execute_query(
        query,
        database_="neo4j"
    )
    print(records)
    return [record["username"] for record in records]


@measure
def get_recommendations_for_user(username):
    """
    Return up to 10 movies based on similar users taste.
    """
    query = """
        MATCH (u1:User {username: $username})-[:LIKES]->(m1:Movie)
        WITH u1, collect(m1) AS u1_movies

        MATCH (u2:User)-[:LIKES]->(m2:Movie)
        WHERE u2 <> u1 AND m2 IN u1_movies
        WITH u1, u1_movies, u2, collect(m2) AS u2_movies

        WITH u1, u1_movies, u2, u2_movies,
             size([movie IN u1_movies WHERE movie IN u2_movies]) AS intersection,
             size(u1_movies) + size(u2_movies) - size([movie IN u1_movies WHERE movie IN u2_movies]) AS union
        ORDER BY toFloat(intersection) / union DESC
        LIMIT 10
        WITH u1, u2

        MATCH (u2)-[:LIKES]->(rec_movie:Movie)
        WHERE NOT EXISTS((u1)-[:LIKES]->(rec_movie))
        WITH rec_movie, count(DISTINCT u2) AS neighbor_likes // assuming the likes are based on users from neo4j

        ORDER BY neighbor_likes DESC
        LIMIT 10
        RETURN rec_movie._id AS movie_id
    """
    records, _, _ = DB.neo4j_driver.execute_query(query, username=username, database_="neo4j")
    
    # Convert string IDs from Neo4j to integers for the MongoDB query
    recommended_ids = [int(record["movie_id"]) for record in records]

    if not recommended_ids:
        return []

    # Fetch full movie details from MongoDB
    return list(DB.mongo_db().movies.find(
        {"_id": {"$in": recommended_ids}},
        {
            "_id": 1,
            "poster_path": 1,
            "release_date": 1,
            "title": 1,
            "vote_average": 1,
            "vote_count": 1
        }
    ))


def get_metrics(metrics_names: List) -> Dict[str, Tuple[float, float]]:
    """
    Return 90th and 95th percentile in seconds for each one of the given metric names using T-Digest.
    """
    metrics = {}
    for name in metrics_names:
        if DB.redis_db(decoded=True).exists(name):
            p90, p95 = DB.redis_db(decoded=True).execute_command('TDIGEST.QUANTILE', name, 0.9, 0.95)

            p90_val = float(p90) if p90 != 'nan' else 0
            p95_val = float(p95) if p95 != 'nan' else 0
            metrics[name] = (p90_val, p95_val)
        else:
            metrics[name] = (0, 0)
    return metrics


def store_metric(metric_name: str, measure_s: float):
    """
    Store measured sample in seconds of the given metric using T-Digest.
    """
    if not DB.redis_db(decoded=True).exists(metric_name):
        DB.redis_db().execute_command('TDIGEST.CREATE', metric_name)
    DB.redis_db().execute_command('TDIGEST.ADD', metric_name, measure_s)
