lines = LOAD '/user/root/pigInputs/pig_input.txt.gz' using PigStorage() AS (line:chararray);
tokenized_words = FOREACH lines GENERATE TOKENIZE(line) AS word;
DESCRIBE tokenized_words;
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

grp_data = GROUP words BY word;
wordcount = FOREACH grp_data GENERATE group, COUNT(words);
EXPLAIN wordcount;
--wordcount_sorted = ORDER wordcount BY $1;
--EXPLAIN wordcount_sorted;
