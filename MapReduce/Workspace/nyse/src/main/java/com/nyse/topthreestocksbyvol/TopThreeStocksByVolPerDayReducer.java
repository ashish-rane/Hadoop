package com.nyse.topthreestocksbyvol;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.nyse.counters.enums.NoTradeDays;
import com.nyse.keyvalues.LongPair;

public class TopThreeStocksByVolPerDayReducer extends Reducer<LongPair, Text, LongPair, Text> {

	private MultipleOutputs<LongPair, Text> multipleOutputs;
	

	@Override
	protected void setup(Reducer<LongPair, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {
	
		multipleOutputs = new MultipleOutputs<LongPair, Text>(context);
		
	}

	@Override
	protected void reduce(LongPair key, Iterable<Text> values,
			Reducer<LongPair, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {

		int count = 0;
		for(Text v: values)
		{
			// Just write the first 3 records since we have the values already
			// sorted in descending order by our Grouping and Sorting comparator
			count++;
			if(count > 3)
				break;
		
			if(key.getSecond() == 0){
				// increment the custom counter for No trade days if the first record has the volume 0
				//context.getCounter(NoTradeDays.NO_TRADE_DAYS).increment(1);
				
				// dynamic counter
				context.getCounter("NoTradeDays", "NO_TRADE_DAYS").increment(1);
				break;
			}
			
			/**
			 * Multiple output allow as to write K,V to a unique file based on the 3rd parameter
			 * which specifies the base output path. (which acts as prefix for the filename.
			 * in this case we are writing records based on the first part of the key (i.e date) 
			 */
			multipleOutputs.write(key, v, Long.toString((long)(key.getFirst()/10000)));
			
			
			// In this case it will create a directory for each year.
			//String basePath = String.format("%s/part", Long.toString((long)(key.getFirst()/10000)));
			//multipleOutputs.write(key, v, basePath);
		}
	}




	@Override
	protected void cleanup(
			Reducer<LongPair, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {
	
		multipleOutputs.close();
	}

	
	
}
