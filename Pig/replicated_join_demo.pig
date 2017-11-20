
stock_record = LOAD '/user/root/datafiles/nyse/nyse_2014.csv' USING PigStorage(',') AS (stock_ticker:chararray, trade_date:chararray, open_price:float, high_price:float, low_price:float, close_price:float, volume:long);



companylist = LOAD '/user/root/datafiles/companylist/companylist_noheader.csv' USING PigStorage('|') AS (stock_ticker:chararray, remainder:chararray);
--    company_name:chararray,
--    stock_price:chararray,
--    revenue:chararray,
--    some_col:chararray,
--    year:chararray,
--    sector:chararray, 
--    industry:chararray,
--    website:chararray, 
--    extra:chararray);

--replicated_join = JOIN nysestocks BY stock_ticker LEFT, companylist BY stock_ticker USING 'replicated' PARALLEL 2;
ILLUSTRATE companylist;
--ILLUSTRATE replicated_join;
