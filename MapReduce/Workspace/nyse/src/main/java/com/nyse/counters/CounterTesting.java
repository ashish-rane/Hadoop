package com.nyse.counters;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CounterTesting extends Configured implements Tool{

	public int run(String[] args) throws Exception {

		String jobId = args[0];
		String groupName = args[1];
		String counterName = args[2];
		
		Cluster cluster = new Cluster(getConf());
		Job job = cluster.getJob(JobID.forName(jobId));
		
		Counters counters = job.getCounters();
		System.out.println(counters.findCounter(groupName, counterName).getValue());
		
		return 0;
	}

	
	public static void main(String[] args) throws Exception {
		
		System.exit(ToolRunner.run(new CounterTesting(), args));
	}
	
}
