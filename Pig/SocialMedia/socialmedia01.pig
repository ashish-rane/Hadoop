REGISTER /usr/hdp/2.3.0.0-2557/pig/lib/piggybank.jar;
DEFINE CSV org.apache.pig.piggybank.storage.CSVExcelStorage();

answers = LOAD '/user/root/datafiles/stackexchange/answers.csv' USING CSV AS (col1:int, 
	question_id:long, userid_question:long, question_score:int, question_unix_time:long, 
	--tags:tuple(tag1:chararray, tag2:chararray, tag3: chararray, tag4:chararray, tag5: chararray, tag6:chararray),
	tags:chararray,
	question_views:long,
	question_answers:long, answer_id:long, userid_answer:long, answer_score:int, answer_unix_time) ;

tag_list = FOREACH answers GENERATE STRSPLITTOBAG(tags,',', 0) AS tag_bag;

tags_flat = FOREACH tag_list GENERATE flatten(tag_bag) AS tag;

tags_filtered = FILTER tags_flat BY (tag is not null AND tag != '');

tags_grp = GROUP tags_filtered BY tag;


tags_count = FOREACH tags_grp GENERATE group, COUNT(tags_filtered.tag) as cnt;

tags_sorted = ORDER tags_count BY cnt DESC;
tags_top10 = LIMIT tags_sorted 10;

DUMP tags_top10;

