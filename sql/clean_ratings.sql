DROP TABLE IF EXISTS clean.ratings;

CREATE TABLE clean.ratings AS
SELECT
    user_id,
    movie_id,
    rating,
    rating_timestamp
FROM staging.ratings
WHERE rating IS NOT NULL
  AND rating BETWEEN 0.5 AND 5.0
  AND rating_timestamp IS NOT NULL;

CREATE INDEX idx_clean_ratings_movie
ON clean.ratings(movie_id);

CREATE INDEX idx_clean_ratings_time
ON clean.ratings(rating_timestamp);
