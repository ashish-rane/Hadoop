package com.nyse.avgstockvolpermonth;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;

public class AvgStockVolPerMonthReducer extends Reducer<TextPair, LongPair, TextPair, LongPair>{

	
	Logger log = Logger.getLogger(AvgStockVolPerMonthReducer.class);
	
	@Override
	protected void reduce(TextPair key, Iterable<LongPair> values,
			Reducer<TextPair, LongPair, TextPair, LongPair>.Context context)
			throws IOException, InterruptedException {
			
		Long totalVolume = new Long(0);
		Long noOfRecords = new Long(0);
		
		for(LongPair item: values){
			
			totalVolume += item.getFirst();
			noOfRecords += item.getSecond();
		}
		
		Long avgVolume = totalVolume/noOfRecords;
		
		LongPair result = new LongPair(avgVolume, noOfRecords);
		context.write(key, result);
		
	}

	
	
}
