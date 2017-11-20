REGISTER  '/usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar'
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD 'datafiles/stackexchange' USING CSV AS (id:int, question_id:long,  userid_question:long, question_score:int, question_time:long, 
	tags:chararray,
	question_views:long,
	question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_time);

answers_tags = FOREACH answers GENERATE flatten(STRSPLITTOBAG(tags,',',0)) AS tag:chararray;

tags_grp = GROUP answers_tags BY tag;

tags_count =  FOREACH tags_grp GENERATE group, COUNT(answers_tags.tag) AS cnt;

tags_sorted = ORDER tags_count BY cnt DESC;
top10 = LIMIT tags_sorted 10;

DUMP top10;



