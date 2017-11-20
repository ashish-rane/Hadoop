batting = LOAD 'pigInputs/batting.csv' USING  PigStorage(',') AS (player:chararray, year:int, runs:int);

batting_filt = FILTER batting BY year > 0;

grp_year = GROUP batting_filt BY year;

max_runs_yr = FOREACH grp_year GENERATE group AS yr, MAX(batting_filt.runs) as maxruns;

join_batting = JOIN max_runs_yr BY (yr, maxruns), batting BY (year,runs);

max_runs_player = FOREACH join_batting GENERATE player, year, maxruns AS runs;
DUMP max_runs_player;

