set default_parallel 1;

stocks = LOAD 'datafiles/nyse/nyse*' USING PigStorage(',') AS (sym:chararray, date:chararray, open, high, low, close, volume:int);


stocks_grp = GROUP stocks BY ToString(ToDate(date,'dd-MMM-yyyy'), 'yyyy-MM-dd');

top3_day = FOREACH stocks_grp {
	
	ordered = ORDER stocks BY volume;
	top3 = LIMIT ordered 3;
	GENERATE group, flatten(top3);
	
}; 


STORE top3_day INTO 'output/pig/nyse_top3stocksbyvolperday02';

--limit100 = LIMIT  top3_day 100;
--DUMP limit100;



