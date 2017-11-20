package com.nyse.topthreestocksbyvol;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.nyse.comparators.LongPairGroupingComparator;
import com.nyse.comparators.LongPairSortingComparator;
import com.nyse.keyvalues.LongPair;
import com.nyse.partitioners.FirstKeyLongPairPartitioner;



public class TopThreeStocksByVolPerDayCompressDriver extends Configured implements Tool{

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
			System.out.println(p.toString());
		}
		
		// Set Output Path
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		// compress  output
		FileOutputFormat.setCompressOutput(job, true);	
		// use snappy for compression
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		// Set Input Format. The actual files can be compressed and TextInputFormat will handle this.
		// as long as the contents of compressed files are compatible with input format specified below.
		job.setInputFormatClass(TextInputFormat.class);
		
		System.out.println("Set the Output format to text");
		
		// Set Map Output Key and Value
		job.setMapOutputKeyClass(LongPair.class);
		job.setMapOutputValueClass(Text.class);
		
		//conf.setBoolean("mapreduce.map.output.compress", true);
		//conf.setClass("mapreduce.map.output.compress.codec", SnappyCodec.class, CompressionCodec.class);
		
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

		System.exit(ToolRunner.run(new TopThreeStocksByVolPerDayCompressDriver(), args));

	}

	

}
