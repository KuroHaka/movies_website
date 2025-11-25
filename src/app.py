from flask import Flask, render_template, request, session, redirect

from .service import (
    get_top_rated_movies,
    get_movie_details,
    get_recent_released_movies,
    get_recommendations_for_user,
    get_same_genres_movies,
    get_similar_movies,
    search_movie,
    get_movie_likes,
    store_metric,
    get_metrics,
)

app = Flask(__name__)

app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'


@app.route("/", methods=["GET"])
def index():
    top_rated_movies = get_top_rated_movies()
    recent_movies = get_recent_released_movies()
    if username := session.get("username"):
        recommendations = get_recommendations_for_user(username)
    else:
        recommendations = []
    return render_template(
        "index.html",
        top_rated_movies=top_rated_movies,
        recent_movies=recent_movies,
        recommendations=recommendations,
        metrics=get_metrics(
            [
                "get_top_rated_movies",
                "get_recent_released_movies",
                "get_recommendations_for_user",
            ]
        ),
    )


@app.route("/login", methods=["POST"])
def login():
    session["username"] = request.form["username"]
    # Redirect to the page the user was on, or to the homepage
    return redirect(request.referrer or "/")


@app.route("/logout")
def logout():
    session.pop("username", None)
    return redirect("/")


@app.route("/search")
def search_results():
    text = request.args["query"]
    result = search_movie(text)
    search_results = result.pop("searchResults")
    return render_template(
        "list.html",
        search_results=search_results,
        votes_facet=result["votesFacet"],
        release_year_facet=result["releaseYearFacet"],
        genre_facet=result["genreFacet"],
        metrics=get_metrics(["search_movie"]),
    )


@app.route("/movie/<movie_id>")
def movie_details(movie_id):
    movie_id = int(movie_id)
    movie = get_movie_details(movie_id)
    if movie and (username := session.get("username")):
        likes = get_movie_likes(username, movie_id)
    else:
        likes = []
    likes = ", ".join(likes)

    if movie:
        same_genre = get_same_genres_movies(movie_id, movie["genres"])
        similar = get_similar_movies(movie_id)
    else:
        same_genre = []
        similar = []

    return render_template(
        "details.html",
        movie=movie,
        same_genre=same_genre,
        similar=similar,
        likes=likes,
        metrics=get_metrics(
            [
                "get_movie_details",
                "get_movie_likes",
                "get_similar_movies",
            ]
        ),
    )
