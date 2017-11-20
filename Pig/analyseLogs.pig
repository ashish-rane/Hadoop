REGISTER '/root/Projects/udfs-0.0.1-SNAPSHOT.jar';
DEFINE validDate com.xpert.pig.udfs.VALID_DATE();

logs = LOAD '/user/root/datafiles/logdata/' USING PigStorage(' ') AS (logdate:chararray, time:chararray, thread:chararray, level:chararray, logger:chararray, message:chararray);
logs_filtered =  FILTER logs BY validDate(SPRINTF('%s %s',logdate, time), 'yyyy-MM-dd HH:mm:ss,SSS') is not null AND (level == 'ERROR' OR level == 'WARN') ;
DESCRIBE logs_filtered;

grouped_by_level = GROUP logs_filtered BY level;

--counts_by_level = FOREACH grouped_by_level GENERATE group as level, COUNT_STAR(logs_filtered);
-- OR
counts_by_level = FOREACH grouped_by_level GENERATE group as level, COUNT(logs_filtered.level);
--ILLUSTRATE counts_by_level;
DUMP counts_by_level;
