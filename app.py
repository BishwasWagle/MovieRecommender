from flask import Blueprint, session, render_template, redirect
import pandas as pd
import pandasql

main = Blueprint('main', __name__)

import json
from engine import RecommendationEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id, count)
    return json.dumps(top_ratings)


@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
    return json.dumps(ratings)


@main.route("/<int:user_id>/ratings", methods=["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list)
    # create a list with the format required by the negine (user_id, movie_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)

    return json.dumps(ratings)


################### Mine Custom Functions ###############################

@main.route("/", methods=["GET"])
def home_page():
    if 'user_id' in session:
        old_user = True
        user_id = session['user_id']
    else:
        old_user = False
        f = open("maxid.txt", "r+")
        user_id = int(f.readline()) + 1
        f.seek(0)
        f.write(str(user_id))
        f.close()
        session['user_id'] = user_id

    ratings_exist = session.get('ratings_exist', False)
    return render_template('home.html', old_user=old_user, ratings_exist=ratings_exist)


@main.route("/search", methods=["GET"])
def search_movie():
    movies = pd.read_csv("./datasets/ml-latest/movies.csv")
    query = request.args.get('q')
    sql = "SELECT movieId as id, title as text from movies where lower(title) like '%" + query.lower() + "%'"
    result = pandasql.sqldf(sql, locals())

    return result.to_json(orient="records")


@main.route("/post-ratings", methods=["POST"])
def post_ratings():
    json_string = request.form.get('mero_data', '[]')
    obj = json.loads(json_string)['user_data']
    print(obj)
    ratings = []
    for (index, x) in obj.items():
        print(x)
        ratings.append((session['user_id'], int(x['movie']), float(x['rating'])))

    # add them to the model using then engine API
    print(ratings)
    recommendation_engine.add_ratings(ratings)
    session['ratings_exist'] = True
    return "lol"


@main.route("/top-recommend/<int:count>")
def top_recommend(count):
    if 'user_id' not in session:
        return redirect('/')

    logger.debug("User %s TOP ratings requested", session['user_id'])
    top_ratings = recommendation_engine.get_top_ratings(session['user_id'], count)
    print(top_ratings)
    return render_template("recommend.html", top_ratings=top_ratings)


def create_app(spark_context, dataset_path):
    global recommendation_engine

    recommendation_engine = RecommendationEngine(spark_context, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    return app
