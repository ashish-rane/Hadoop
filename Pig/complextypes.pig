--stocks = LOAD 'datafiles/hirw/stocks' USING PigStorage AS (exch:chararray, symbol:chararray, ymd:datetime, open:float, high:float, low:float, close:float, volume:int, adj_close:float);

--stocks_grp = GROUP stocks BY (symbol, GetYear(ymd));

--STORE stocks_grp INTO 'output/hirw/pig/complexdata' ;

--complex_type = LOAD 'output/hirw/pig/complexdata' USING PigStorage(',') AS ( grp:(sym:chararray, year:int), stocks_b:{ stocks_t:(exch, symbol:chararray, ymd:datetime, open, high, low, close:float, volume:int, adj_close ) });


--max_vol = FOREACH complex_type GENERATE grp.sym, grp.year, MAX(stocks_b.volume) as maxvol;

--limit10 = limit max_vol 10;

--DUMP limit10;


stocks = LOAD 'datafiles/hirw/stocks' USING PigStorage(',') as (exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float,
volume:int, adj_close:float);

grp_by_sym_yr = GROUP stocks BY (symbol, GetYear(date));

STORE grp_by_sym_yr INTO 'output/hirw/pig/complexdata' USING PigStorage(',') ;

complex_type = LOAD 'output/hirw/pig/complexdata' as  (grp:tuple(sym:chararray, yr:int), stocks_b:{stocks_t:(exchange:chararray, symbol:chararray, date:datetime, open:float, high:float, low:float, close:float, volume:int, adj_close:float)}) ;

max_vol_by_yr = FOREACH complex_type GENERATE grp.sym, grp.yr, MAX(stocks_b.volume);
limit10 = LIMIT max_vol_by_yr 10;
DUMP limit10;

