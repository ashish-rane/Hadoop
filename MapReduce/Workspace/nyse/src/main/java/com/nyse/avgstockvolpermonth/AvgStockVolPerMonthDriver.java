package com.nyse.avgstockvolpermonth;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;
import com.nyse.partitioners.FirstKeyTextPairPartitioner;
import com.nyse.partitioners.SecondKeyTextPairPartitioner;

/**
 * 
 * @author Ashish.Rane
 * Usage : yarn jar nyse*.jar -Dfilter.by.stockticker=BAC 
 *
 */

public class AvgStockVolPerMonthDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		// Set the class to run 
		job.setJarByClass(getClass());
		
		
		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
		Path path = new Path(args[0] + args[1]);
		
		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		
		for(Path p: paths){
			FileInputFormat.addInputPath(job, p);
		}
		
		
		// Set Input and Output Path
		//FileInputFormat.setInputPaths(job, args[0]);
		
		FileOutputFormat.setOutputPath(job, new Path( args[2]));
		
		// Set input format
		// Combine the small file into one big file
		job.setInputFormatClass(CombineTextInputFormat.class);
	
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(LongPair.class);
		
		// set Mapper
		job.setMapperClass(AvgStockVolPerMonthMapper.class);
	
		// either pass it in command line using -Dpartition.by OR add this property in the configuration.xml
		if(conf.get("partition.by").equals("trade_month"))
		{
			job.setPartitionerClass(FirstKeyTextPairPartitioner.class);
		}
		else
		{
			job.setPartitionerClass(SecondKeyTextPairPartitioner.class);
		}
		
		// set Combiner
		job.setCombinerClass(AvgStockVolPerMonthCombiner.class);
		
		// set reducer
		job.setReducerClass(AvgStockVolPerMonthReducer.class);
		job.setNumReduceTasks(4);
		
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(LongPair.class);
		
		return job.waitForCompletion(true)? 0: 1;
	}

	
	public static void main(String[] args) throws Exception{
		
		
		System.exit(ToolRunner.run(new AvgStockVolPerMonthDriver(), args));
	}
	
}
