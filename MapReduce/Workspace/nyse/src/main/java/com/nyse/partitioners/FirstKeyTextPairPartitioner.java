package com.nyse.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;

public class FirstKeyTextPairPartitioner extends Partitioner<TextPair, LongPair> {

	@Override
	public int getPartition(TextPair key, LongPair value, int numPartitions) {
		int result = 0;
		
		// Hash code will result in a skew of the output, which means different output files will 
		//have same YYYY-MM for different stocks.
		//result = (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
		// by converting YYYY-MM to YYYYMM and using it as the integer to be used to determine partitions
		// this will group all the same month and year for all stocks in the same output file. 
		result = new Integer(key.getFirst().toString().replace("-", "")).intValue() % numPartitions;
		
		return result;
	}

}
