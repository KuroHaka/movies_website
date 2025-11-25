from pymongo import MongoClient
from redis import Redis
from neo4j import GraphDatabase

class Database:
    def __init__(self):
        self._mongoDB = MongoClient().moviesdb
        self._redisDB = Redis()
        self._decodedRedisDB = Redis(decode_responses=True)
        self.neo4j_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))


    def redis_db(self, decoded=False):
        if decoded:
            return self._decodedRedisDB
        return self._redisDB

    def mongo_db(self):
        return self._mongoDB
