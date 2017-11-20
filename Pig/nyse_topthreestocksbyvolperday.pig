stock_record = LOAD '/user/root/datafiles/nyse/nyse*' USING PigStorage(',') AS (stock_ticker:chararray, trade_date:chararray, open_price:float, high_price:float, low_price:float, close_price:float, volume:long);
grp_by_date  = group stock_record by ToString(ToDate(trade_date, 'dd-MMM-yyyy' ), 'yyyy-MM-dd');
DESCRIBE grp_by_date
top_three_by_vol = FOREACH grp_by_date{
	
	sorted = order stock_record  by volume DESC;
	top3 = LIMIT sorted 3;
	GENERATE group, FLATTEN(top3);
};
top_three_by_vol_sorted = ORDER top_three_by_vol BY group;
ILLUSTRATE top_three_by_vol_sorted;
--STORE top_three_by_vol_sorted  INTO '/user/root/output/pig/nyse/top_three_by_vol_per_day' USING PigStorage(',');
