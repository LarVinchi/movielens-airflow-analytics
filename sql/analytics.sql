-- name: top_10_movies_by_avg_rating
SELECT
    m.title,
    COUNT(f.rating) AS num_ratings,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact.fact_ratings f
JOIN dim.dim_movie m ON f.movie_id = m.movie_id
WHERE f.rating_time >= TO_DATE(%(start)s, 'YYYY-MM-DD') 
  AND f.rating_time <  TO_DATE(%(end)s, 'YYYY-MM-DD')
GROUP BY m.movie_id, m.title
HAVING COUNT(f.rating) >= 5
ORDER BY avg_rating DESC
LIMIT 10;

-- name: least_10_movies_by_avg_rating
SELECT
    m.title,
    COUNT(f.rating) AS num_ratings,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact.fact_ratings f
JOIN dim.dim_movie m ON f.movie_id = m.movie_id
WHERE f.rating_time >= TO_DATE(%(start)s, 'YYYY-MM-DD')
  AND f.rating_time <  TO_DATE(%(end)s, 'YYYY-MM-DD')
GROUP BY m.movie_id, m.title
HAVING COUNT(f.rating) >= 5
ORDER BY avg_rating ASC
LIMIT 10;

-- name: top_5_genres_by_ratings
SELECT
    genre,
    COUNT(*) AS total_ratings
FROM fact.fact_ratings f
JOIN dim.dim_movie m ON f.movie_id = m.movie_id
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
WHERE f.rating_time >= TO_DATE(%(start)s, 'YYYY-MM-DD')
  AND f.rating_time <  TO_DATE(%(end)s, 'YYYY-MM-DD')
GROUP BY genre
ORDER BY total_ratings DESC
LIMIT 5;

-- name: least_5_genres_by_ratings
SELECT
    genre,
    COUNT(*) AS total_ratings
FROM fact.fact_ratings f
JOIN dim.dim_movie m ON f.movie_id = m.movie_id
CROSS JOIN LATERAL unnest(string_to_array(m.genres, '|')) AS genre
WHERE f.rating_time >= TO_DATE(%(start)s, 'YYYY-MM-DD')
  AND f.rating_time <  TO_DATE(%(end)s, 'YYYY-MM-DD')
GROUP BY genre
ORDER BY total_ratings ASC
LIMIT 5;