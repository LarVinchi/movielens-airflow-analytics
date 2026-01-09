CREATE SCHEMA IF NOT EXISTS dim;

DROP TABLE IF EXISTS dim.dim_movie;
CREATE TABLE dim.dim_movie AS
SELECT
    movie_id,
    title,
    genres
FROM clean.movies;

ALTER TABLE dim.dim_movie
ADD CONSTRAINT pk_dim_movie PRIMARY KEY (movie_id);


DROP TABLE IF EXISTS dim.dim_user;
CREATE TABLE dim.dim_user AS
SELECT DISTINCT
    user_id
FROM clean.ratings;

ALTER TABLE dim.dim_user
ADD CONSTRAINT pk_dim_user PRIMARY KEY (user_id);


DROP TABLE IF EXISTS dim.dim_date;

CREATE TABLE dim.dim_date AS
SELECT DISTINCT
    DATE(rating_timestamp) AS date
FROM clean.ratings;

ALTER TABLE dim.dim_date
ADD COLUMN date_id INTEGER,
ADD COLUMN year INTEGER,
ADD COLUMN month INTEGER,
ADD COLUMN month_name TEXT,
ADD COLUMN quarter TEXT,
ADD COLUMN day_of_week TEXT,
ADD COLUMN is_weekend BOOLEAN;

UPDATE dim.dim_date
SET
    date_id = TO_CHAR(date, 'YYYYMMDD')::INTEGER,
    year = EXTRACT(YEAR FROM date),
    month = EXTRACT(MONTH FROM date),
    month_name = TO_CHAR(date, 'Month'),
    quarter = 'Q' || EXTRACT(QUARTER FROM date),
    day_of_week = TO_CHAR(date, 'Day'),
    is_weekend = EXTRACT(DOW FROM date) IN (0,6);

ALTER TABLE dim.dim_date
ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_id);

ALTER TABLE dim.dim_date
ADD CONSTRAINT uq_dim_date UNIQUE (date);

SELECT COUNT(*) FROM dim.dim_movie;
SELECT COUNT(*) FROM dim.dim_user;
SELECT COUNT(*) FROM dim.dim_date;
