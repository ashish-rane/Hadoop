stocks = LOAD '$input' USING PigStorage(',') AS (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,volume:int, adj_close:float);
stocks_2003 = FILTER stocks BY GetYear(date) == 2003;
grp_sym = GROUP stocks_2003 BY symbol;
avg_vol = FOREACH grp_sym GENERATE group AS sym, AVG(stocks_2003.volume) AS avgvol;
avg_vol_sort = ORDER avg_vol BY avgvol DESC;
top10 = LIMIT avg_vol_sort 10;
STORE top10 INTO '$output' USING PigStorage(',');

