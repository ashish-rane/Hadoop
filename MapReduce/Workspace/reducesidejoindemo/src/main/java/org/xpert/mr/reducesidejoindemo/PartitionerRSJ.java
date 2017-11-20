package org.xpert.mr.reducesidejoindemo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerRSJ extends Partitioner<CompositeKeyWritableRSJ, Text>{

	@Override
	public int getPartition(CompositeKeyWritableRSJ key, Text value,
			int numPartitions) {
		
		
		// Partitions on join key ( employee id)
		
		return (key.getJoinKey().hashCode() % numPartitions);
	}

}
