-- Task 6: Create a keyspace named entertainment
CREATE KEYSPACE IF NOT EXISTS entertainment WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the keyspace
USE entertainment;

-- Task 7: Create the movies table with all columns as text
CREATE TABLE IF NOT EXISTS movies (
    id text PRIMARY KEY,
    title text,
    year text,
    rating text,
    director text
);

-- Import the CSV data into Cassandra
-- Use the following command in your terminal to import the CSV file
-- cqlsh -u username -p password -e "COPY entertainment.movies (id, title, year, rating, director) FROM '/home/project/partial_data.csv' WITH HEADER = TRUE;"

-- Task 8: Count the number of rows in the movies table
SELECT COUNT(*) FROM entertainment.movies;

-- Task 9: Create an index for the rating column
CREATE INDEX IF NOT EXISTS rating_index ON entertainment.movies (rating);

-- Task 10: Count the number of movies that are rated 'G'
SELECT COUNT(*) FROM entertainment.movies WHERE rating = 'G';
