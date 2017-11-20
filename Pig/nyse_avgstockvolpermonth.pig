stock_record = LOAD '/user/root/datafiles/nyse/nyse_2014.csv' USING PigStorage(',') AS (stock_ticker:chararray, trade_date:chararray, open_price:float, high_price:float, low_price:float, close_price:float, volume:long);
stock_record_with_month  = FOREACH stock_record GENERATE CONCAT(ToString(ToDate(trade_date, 'dd-MMM-yyyy' ), 'yyyy-MM'), ' ', stock_ticker) AS key, volume;
DESCRIBE stock_record_with_month;
grp_data = GROUP stock_record_with_month by(key);
DESCRIBE grp_data;
avg_vol_per_month = FOREACH grp_data GENERATE group as trade_month, AVG(stock_record_with_month.volume) AS avg_vol;
--STORE avg_vol_per_month INTO '/user/root/output/pig/nyse/avg_vol_per_month' USING PigStorage(',');
ILLUSTRATE avg_vol_per_month;
