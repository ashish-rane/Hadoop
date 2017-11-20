package com.nyse.partitioners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.nyse.keyvalues.LongPair;

public class FirstKeyLongPairPartitioner  extends Partitioner<LongPair, Text>{

	@Override
	public int getPartition(LongPair key, Text value, int numPartitions) {
	 

		return (int)((long)(key.getFirst()/10000) % numPartitions);
	}

}
