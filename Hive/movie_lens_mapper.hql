use movielens;

CREATE TABLE u_data_new (user_id int , movie_id int, rating int, weekday int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

add FILE weekday_mapper.py;

INSERT OVERWRITE TABLE u_data_new
SELECT 
	TRANSFORM (user_id, movie_id, rating, unixtime)
	USING 'python weekday_mapper.py'
	AS (user_id, movie_id, rating, weekday)
FROM u_data;

SELECT weekday, count(*)
FROM u_data_new 
GROUP BY weekday;
