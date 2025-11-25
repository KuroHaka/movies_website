#!/bin/bash

# Start MongoDB service
sudo systemctl start mongod

# --- Container Configuration ---
REDIS_CONTAINER_NAME="redis-stack"
NEO4J_CONTAINER_NAME="neo4j-db"

# --- Start Redis Container ---
if ! docker ps --filter "name=$REDIS_CONTAINER_NAME" --filter "status=running" | grep -q "$REDIS_CONTAINER_NAME"; then
    docker start "$REDIS_CONTAINER_NAME" >/dev/null 2>&1 || \
    docker run -d --name "$REDIS_CONTAINER_NAME" -p 6379:6379 -p 8001:8001 redis/redis-stack:latest >/dev/null
    sleep 5 
fi

# --- Start Neo4j Container ---
if ! docker ps --filter "name=$NEO4J_CONTAINER_NAME" --filter "status=running" | grep -q "$NEO4J_CONTAINER_NAME"; then
    docker start "$NEO4J_CONTAINER_NAME" >/dev/null 2>&1 || \
    docker run -d \
        --name "$NEO4J_CONTAINER_NAME" \
        -p 7474:7474 -p 7687:7687 \
        -e NEO4J_AUTH=neo4j/password \
        neo4j:latest >/dev/null
    sleep 10
fi

# --- Run Flask Application ---
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
export PYTHONPATH="${PYTHONPATH}:${SCRIPT_DIR}/movies_website_src"
export FLASK_APP=movies_website_src/app.py
flask run --debug --port 5001

