batting = LOAD '/user/guest/Batting.csv' USING PigStorage(',');
transform = FOREACH batting GENERATE $0 as playerID, $1 as year, $8 as runs;
STORE transform INTO 'pigInputs/' using PigStorage(',');
