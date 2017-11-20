CREATE DATABASE stackexchange;


USE stackexchange;

CREATE EXTERNAL TABLE stackexchange.answers_stg 
(
	id INT, question_id BIGINT, userid_question INT, question_score TINYINT, question_time BIGINT, tags STRING,
	question_views INT, question_answers INT, answer_id BIGINT, userid_answer INT, answer_score TINYINT, answer_time BIGINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/user/root/datafiles/stackexchange/';


CREATE TABLE stackexchange.answers
(
        id INT, question_id BIGINT, userid_question INT, question_score TINYINT, question_time BIGINT, tags ARRAY<STRING>,
        question_views INT, question_answers INT, answer_id BIGINT, userid_answer INT, answer_score TINYINT, answer_time BIGINT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
COLLECTION ITEMS TERMINATED BY '|'
STORED AS TEXTFILE;


SELECT tag, count(*) as cnt  FROM answers LATERAL VIEW explode(tags) tagTable AS tag GROUP BY tag ORDER BY cnt DESC LIMIT 10;
