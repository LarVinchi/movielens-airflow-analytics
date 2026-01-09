DROP TABLE IF EXISTS clean.movies;

CREATE TABLE clean.movies AS
SELECT
    movie_id,
    TRIM(title) AS title,
    genres
FROM staging.movies
WHERE movie_id IS NOT NULL
  AND title IS NOT NULL;

ALTER TABLE clean.movies
ADD CONSTRAINT pk_clean_movies PRIMARY KEY (movie_id);
