// Connect to MongoDB
// Use the following command in your terminal to import the JSON file
// mongoimport -u root -p Mjc5NzUtb3BleWVt --authenticationDatabase admin --db entertainment --collection movies --file /home/project/movies.json --jsonArray

// Queries to run in MongoDB shell (mongosh)
use entertainment;

// Task 2: Find the year in which the most number of movies were released
db.movies.aggregate([
    { $group: { _id: "$year", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: 1 }
]);

// Task 3: Find the count of movies released after the year 1999
db.movies.countDocuments({ year: { $gt: 1999 } });

// Task 4: Find the average votes for movies released in 2007
db.movies.aggregate([
    { $match: { year: 2007 } },
    { $group: { _id: null, averageVotes: { $avg: "$votes" } } }
]);

// Task 5: Export specified fields to a CSV file
// Use the following command in your terminal to export the data
// mongoexport --uri="mongodb://root:Mjc5NzUtb3BleWVt@localhost:27017" --authenticationDatabase=admin --db=entertainment --collection=movies --type=csv --fields=_id,title,year,rating,director --out=/home/project/partial_data.csv
