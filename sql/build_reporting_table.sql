-- Clean up previous version of the table
DROP TABLE IF EXISTS analytics.reporting_master;

-- Create a Master Reporting Table
-- This essentially "flattens" your star schema for easier BI/Reporting
CREATE TABLE analytics.reporting_master AS
SELECT 
    f.rating_id,
    f.user_id,
    f.rating,
    f.rating_time,
    m.movie_id,
    m.title,
    m.genres,
    m.release_year
FROM fact.fact_ratings f
JOIN dim.dim_movie m ON f.movie_id = m.movie_id;

-- Create indexes to make date-based filtering instant
CREATE INDEX idx_reporting_time ON analytics.reporting_master(rating_time);
CREATE INDEX idx_reporting_movie_id ON analytics.reporting_master(movie_id);