batting = LOAD 'pigInputs/batting.csv' USING PigStorage(',') AS (playerID:chararray, year:int, runs:int)  ;
runs = FILTER batting BY $1 > 0;
grp_data = GROUP runs by(year);
max_runs = FOREACH grp_data GENERATE group as grp, MAX(runs.runs) AS max_runs;
join_max_runs = JOIN max_runs BY (grp, max_runs), runs BY(year, runs);
DESCRIBE join_max_runs;
join_data = FOREACH join_max_runs GENERATE grp, playerID, max_runs ;
STORE join_data INTO '/user/root/output/pig/batting' USING PigStorage();
