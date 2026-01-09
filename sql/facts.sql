CREATE SCHEMA IF NOT EXISTS fact;

DROP TABLE IF EXISTS fact.fact_ratings;

CREATE TABLE fact.fact_ratings AS
SELECT
    row_number() OVER () AS rating_id,
    user_id,
    movie_id,
    rating,
    rating_timestamp AS rating_time
FROM clean.ratings;

ALTER TABLE fact.fact_ratings
ADD CONSTRAINT pk_fact_ratings PRIMARY KEY (rating_id);
