REGISTER  '/usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar'
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD 'datafiles/stackexchange' USING CSV AS (id:int, question_id:long,  userid_question:long, question_score:int, question_time:long,
        tags:chararray,
        question_views:long,
        question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_time);

answers_filtered = FILTER answers BY id is not null;

questions_grp = GROUP answers_filtered BY question_id;

questions_proj = FOREACH questions_grp GENERATE group, MIN(answers_filtered.question_time) AS min_question, MIN(answers_filtered.answer_time) AS min_answer;

answer_intervals = FOREACH questions_proj GENERATE group, (min_answer - min_question) AS answer_time;

within_hour = FILTER answer_intervals BY answer_time <= 3600;

join_answers = JOIN within_hour BY group, answers_filtered BY question_id;

tags_answer = FOREACH join_answers GENERATE flatten(STRSPLITTOBAG(answers_filtered::tags,',',0)) AS tag;

distinct_tags = DISTINCT tags_answer;

tags_sorted = ORDER distinct_tags BY tag;

DUMP tags_sorted;


