"""Utility file to seed ratings database from MovieLens data in seed_data/"""

# Module imports
from datetime import datetime
from sqlalchemy import func

# Imports from model.py
from model import User, Rating, Movie, connect_to_db, db
from server import app


def load_users():
    """Load users from u.user into database."""

    print("Users")

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    User.query.delete()

    # Read u.user file and insert data
    for row in open("seed_data/u.user"):
        row = row.rstrip()
        user_id, age, gender, occupation, zipcode = row.split("|")

        user = User(user_id=int(user_id),
                    age=int(age),
                    zipcode=zipcode)

        # We need to add to the session, or it won't ever be stored
        db.session.add(user)

    # Once we're done, we should commit our work
    db.session.commit()


def load_movies():
    """Load movies from u.item into database."""

    print("Movies")

    # Delete all rows in the table to prevent duplicates
    Movie.query.delete()

    # Read u.item file and insert data
    for row in open("seed_data/u.item"):
        row = row.rstrip()
        movie_id, title, released_at, _, imdb_url = row.split("|")[:5]

        # Remove year from title and imdb_url
        title = title[:-7]
        imdb_url = imdb_url[:-6]

        # Convert released_at to datetime object
        if released_at:
            format = "%d-%b-%Y"
            released_at = datetime.strptime(released_at, format)
        else:
            released_at = None

        movie = Movie(movie_id=int(movie_id),
                      title=title,
                      released_at=released_at,
                      imdb_url=imdb_url)

        # Add movie to the session
        db.session.add(movie)

    # Commit the changes
    db.session.commit()


def load_ratings():
    """Load ratings from u.data into database."""

    print("Ratings")

    # Delete all rows in the table to prevent duplicates
    Rating.query.delete()

    # Read u.data file and insert data
    for row in open("seed_data/u.data"):
        row = row.rstrip()
        user_id, movie_id, score, _ = row.split("\t")

        rating = Rating(user_id=int(user_id),
                        movie_id=int(movie_id),
                        score=int(score))

        # Add rating to the session
        db.session.add(rating)

    # Commit the changes
    db.session.commit()


def set_val_user_id():
    """Set value for the next user_id after seeding database"""

    # Get the Max user_id in the database
    result = db.session.query(func.max(User.user_id)).one()
    max_id = int(result[0])

    # Set the value for the next user_id to be max_id + 1
    query = "SELECT setval('users_user_id_seq', :new_id)"
    db.session.execute(query, {'new_id': max_id + 1})
    db.session.commit()


if __name__ == "__main__":
    connect_to_db(app)

    # In case tables haven't been created, create them
    db.create_all()

    # Import different types of data
    load_users()
    load_movies()
    load_ratings()
    set_val_user_id()
