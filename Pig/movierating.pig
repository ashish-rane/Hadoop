movies = LOAD 'datafiles/moviedata/movies.dat' USING PigStorage("::") AS (movieId:int, movieName: chararray, genre: chararray);
ratings = LOAD 'datafiles/moviedata/ratings.dat' USING PigStorage("::") AS (userId: int, movieId: int, rating: int, timestamp: chararray);

movierating_group = COGROUP movies BY movieId, ratings BY movieId;
DESCRIBE movierating_group;

