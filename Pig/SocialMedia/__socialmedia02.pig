REGISTER /usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar;
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD '/user/root/datafiles/stackexchange/answers.csv' USING CSV AS (col1:int,
        question_id:long, userid_question:long, question_score:int, question_unix_time:long,
        --tags:tuple(tag1:chararray, tag2:chararray, tag3: chararray, tag4:chararray, tag5: chararray, tag6:chararray),
        tags:chararray,
        question_views:long,
        question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_unix_time:long) ;


grp_question = GROUP answers BY question_id;
DESCRIBE grp_question;
answers_earliest = FOREACH grp_question GENERATE group as question_id, MIN(answers.question_unix_time) AS question_time, MIN(answers.answer_unix_time) AS answer_time;


answers_intervals = FOREACH answers_earliest GENERATE ABS(answer_time - question_time) AS interval;
overall_grp = GROUP answers_intervals ALL;

avgtime_answer = FOREACH overall_grp GENERATE AVG(answers_intervals.interval) AS avgtime;

DUMP avgtime_answer;
