package com.nyse.stockcompanyjoin.distcache;

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

import com.nyse.keyvalues.TextPair;


/**
 * USING HDFS
 * 
 *  yarn jar nyse-0.0.1-SNAPSHOT.jar com.nyse.stockcompanyjoin.distcache.StockCompanyJoinDistCacheDriver 
 *  -files "hdfs://sandbox.hortonworks.com/user/root/datafiles/companylist/companylist_noheader.csv"  
 *  /user/root/datafiles/nyse/ nyse_* /user/root/output/mapreduce/stockcompanyjoindistcache02
 * 
 *  USING LOCAL FILE SYSTEM
 *  
 *  yarn jar nyse-0.0.1-SNAPSHOT.jar com.nyse.stockcompanyjoin.distcache.StockCompanyJoinDistCacheDriver 
 *  -files "/root/Projects/Datafiles/companylist_noheader.csv"  
 *  /user/root/datafiles/nyse/ nyse_* /user/root/output/mapreduce/stockcompanyjoindistcache/
 * 
 */

public class StockCompanyJoinDistCacheDriver  extends Configured implements  Tool{

	
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		Job job = Job.getInstance(conf);
		
		// Set the class to run 
		job.setJarByClass(getClass());
		
		System.out.println("Starting to assign input paths");
		
		FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
		Path path = new Path(args[0] + args[1]);
		
		FileStatus[] status = fs.globStatus(path);
		Path[] paths = FileUtil.stat2Paths(status);
		
		for(Path p: paths){
			System.out.println(p.toString());
			FileInputFormat.addInputPath(job, p);
			
		}
		
	System.out.println("Set the Input Paths");
		
		// Set input format
		// Combine the small file into one single dataset in memory.
		job.setInputFormatClass(CombineTextInputFormat.class);
		
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		
		System.out.println("Setting Mapper...");
		
		// set Mapper
		job.setMapperClass(StockCompanyJoinDistCacheMapper.class);
		
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, new Path( args[2]));
		
		System.out.println("Submitting Job...");
		
		return job.waitForCompletion(true)?0 : 1;
		
		
	}

	
	
	public static void main(String[] args) throws Exception {
		
		System.exit(ToolRunner.run(new StockCompanyJoinDistCacheDriver(), args));

	}

	
}
