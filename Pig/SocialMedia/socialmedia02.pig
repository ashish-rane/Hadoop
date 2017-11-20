--REGISTER /usr/hdp/2.3.4.0-3485/pig/lib/piggybank.jar;
REGISTER /usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar;
REGISTER /root/Projects/Pig/SocialMedia/udfs-0.0.1-SNAPSHOT.jar;


DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();
DEFINE valid_date com.xpert.pig.udfs.VALID_DATE();
DEFINE format_timeinterval com.xpert.pig.udfs.FORMAT_TIME_INTERVAL();

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

-- 5. Calculate the overall average time it took for answer
overall_grp = GROUP answers_intervals ALL;
avgtime_answer = FOREACH overall_grp GENERATE ROUND(AVG(answers_intervals.interval)) AS avgtime;

formatted_time = FOREACH avgtime_answer GENERATE format_timeinterval(avgtime_answer.avgtime) as avgtime;
DUMP formatted_time
--DUMP avgtime_answer;

--STORE avgtime_answer INTO '/user/5b8da33ed8b59b1b37d3fb652c1418/Projects/DataFiles/tmp01' USING PigStorage(',');
