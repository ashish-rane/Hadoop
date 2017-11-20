package com.nyse.topthreestocksbyvol;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.nyse.comparators.LongPairGroupingComparator;
import com.nyse.comparators.LongPairSortingComparator;
import com.nyse.keyvalues.LongPair;
import com.nyse.partitioners.FirstKeyLongPairPartitioner;



public class TopThreeStocksByVolPerDayDriver extends Configured implements Tool{

	private Logger log = Logger.getLogger(getClass());
	

	public int run(String[] args) throws Exception {
		
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		// indicates that the jar that contains this driver class contains 
		// the mapper and reducer and should be used as mapreduce application on the hadoop cluster.
		job.setJarByClass(getClass());
		
		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
		Path path = new Path(args[0] + args[1]);
		
		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		
		for(Path p : paths){
			FileInputFormat.addInputPath(job, p);
		}
		
		// Set Output Path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// Set Input Format
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		
		// Set Map Output Key and Value
		job.setMapOutputKeyClass(LongPair.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set Mapper
		job.setMapperClass(TopThreeStocksByVolPerDayMapper.class);
		
		// Partitioner
		job.setPartitionerClass(FirstKeyLongPairPartitioner.class);
		
		// Grouping Comparator
		job.setGroupingComparatorClass(LongPairGroupingComparator.class);
		
		// Sort comparator
		job.setSortComparatorClass(LongPairSortingComparator.class);
		
	
		
		// Set Reducer
		job.setReducerClass(TopThreeStocksByVolPerDayReducer.class);
		//job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(4);
		
		
		
		job.setOutputKeyClass(LongPair.class);
		job.setOutputValueClass(Text.class);
		
		System.out.println("About to submit job....");
		
		
		log.info("About to submit job...");
		
		
			
		
		return job.waitForCompletion(true)? 0: 1;
		
		/////////////////////////////////////////////////////////////////////////////
		

		
	}
	
	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new TopThreeStocksByVolPerDayDriver(), args));

	}

	

}
