--REGISTER /usr/hdp/2.3.4.0-3485/pig/lib/piggybank.jar;
REGISTER /usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar;

DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

-- 1. Load the data using CSV storage
answers = LOAD 'datafiles/stackexchange/answers.csv' USING CSV AS (col1:int,
        question_id:long, userid_question:long, question_score:int, question_unix_time:long,
        tags:chararray,
        question_views:long,
        question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_unix_time:long) ;

-- 2. Remove Header Row
answers_filtered = FILTER answers BY col1 is not null;

-- 3. For each question find the time of earliest answer
grp_question = GROUP answers_filtered BY question_id;

answers_times = FOREACH grp_question  GENERATE group as question_id, MIN(answers_filtered.question_unix_time) as question_time, MIN(answers_filtered.answer_unix_time) as answer_time;
answers_times_filtered = FILTER answers_times BY (question_id is not null AND question_time is not null AND answer_time is not null);

-- 4. Find the time taken for the earliest answer per question
answers_intervals = FOREACH answers_times GENERATE (answer_time - question_time) AS interval;

-- 5. Filter the intervals which are greater than 1 hour (i.e 3600 secs)
answers_within_hour = FILTER answers_intervals BY (interval <= 3600);

-- 6. Calculate the overall count
overall_grp = GROUP answers_intervals ALL;
num_answers_within_hour = FOREACH overall_grp GENERATE COUNT_STAR(answers_intervals) AS cnt;

DUMP num_answers_within_hour;

