REGISTER  '/usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar'
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD 'datafiles/stackexchange' USING CSV AS (id:int, question_id:long,  userid_question:long, question_score:int, question_time:long,
        tags:chararray,
        question_views:long,
        question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_time);

answers_filtered = FILTER answers BY id is not null;

grp_question = GROUP answers_filtered BY question_id;

question_proj = FOREACH grp_question GENERATE group, MIN(answers_filtered.question_time) as min_question, MIN(answers_filtered.answer_time) AS min_answer;

time_to_answer = FOREACH question_proj GENERATE group ,  min_answer - min_question AS time;
overall_grp = GROUP time_to_answer ALL;

avg_time_to_answer =  FOREACH overall_grp GENERATE ROUND(AVG(time_to_answer.time)) as avgtime:int;

DUMP avg_time_to_answer;
