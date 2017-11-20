package com.nyse.avgstockvolpermonth;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;
import com.nyse.parsers.NYSEParser;

public class AvgStockVolPerMonthMapper  extends Mapper<LongWritable, Text, TextPair, LongPair>{

	private static String FILTER_BY_STOCKTICKER = "filter.by.stockticker";
	private Set<String> filterByStockTicker = new HashSet<String>();
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, TextPair, LongPair>.Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String filter = conf.get(FILTER_BY_STOCKTICKER);
		if(filter != null)
		{
			String[] tickers = filter.split(",");
			for(String s : tickers){
				
				filterByStockTicker.add(s);
			}
		
		}
	}

	@Override
	protected void map(LongWritable lineoffset, Text record,
			Mapper<LongWritable, Text, TextPair, LongPair>.Context context)
			throws IOException, InterruptedException {
		
		NYSEParser parser = new NYSEParser(record.toString());
		
		if(filterByStockTicker.isEmpty() || filterByStockTicker.contains(parser.getStockTicker())){
		
			// Output key
			TextPair key = new TextPair(parser.getTradeMonth(),parser.getStockTicker());
			
			// Output value
			LongPair value = new LongPair(parser.getVolume(), 1L);
			
			context.write(key, value);
		}
	}

	
	
	
}
