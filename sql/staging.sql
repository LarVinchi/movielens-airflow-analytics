CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.movies;
CREATE TABLE staging.movies (
    movie_id INTEGER,
    title TEXT,
    genres TEXT
);

DROP TABLE IF EXISTS staging.ratings;
CREATE TABLE staging.ratings (
    user_id INTEGER,
    movie_id INTEGER,
    rating NUMERIC(2,1),
    rating_timestamp TIMESTAMP
);
