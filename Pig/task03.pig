REGISTER  '/usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar'
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD 'datafiles/stackexchange' USING CSV AS (id:int, question_id:long,  userid_question:long, question_score:int, question_time:long,
        tags:chararray,
        question_views:long,
        question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_time);

answers_filtered = FILTER answers BY id is not null;

grp_question_id = GROUP answers_filtered BY question_id;

question_proj = FOREACH grp_question_id GENERATE group, MIN(answers_filtered.question_time) AS min_question, MIN(answers_filtered.answer_time) AS min_answer;

answer_interval = FOREACH question_proj GENERATE group, (min_answer - min_question) AS answer_time;

within_hour = FILTER answer_interval BY answer_time <= 3600;

overall_grp = GROUP within_hour ALL;

num_question_in_hour = FOREACH overall_grp GENERATE COUNT(within_hour.answer_time) AS cnt;

DUMP num_question_in_hour;

