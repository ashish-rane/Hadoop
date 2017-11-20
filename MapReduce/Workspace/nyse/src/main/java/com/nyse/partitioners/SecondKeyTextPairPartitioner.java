package com.nyse.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;

public class SecondKeyTextPairPartitioner extends Partitioner<TextPair,LongPair>{

	@Override
	public int getPartition(TextPair key, LongPair value, int numPartitions) {

		int result = 0;
		// Get the second field hashcode from TextPair on the basis of which to partition
		result = (key.getSecond().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
		return result;
	}

}
