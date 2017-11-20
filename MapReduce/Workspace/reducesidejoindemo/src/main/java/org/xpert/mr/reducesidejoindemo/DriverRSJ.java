package org.xpert.mr.reducesidejoindemo;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverRSJ  extends Configured implements Tool{

	public int run(String[] args) throws Exception {

		// exit job if arguments not provided
		if(args.length != 3){
			System.out.println("Three parameters are required for DriverRSJ - <inputDir> <inputDir> <outputDir>");
			return -1;
		}
		
		
		// Job instantiation
		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(DriverRSJ.class);
		job.setJobName("ReduceSideJoin");
		
		// Add distributed cache. (Could be also added via command line -files option
		DistributedCache.addCacheArchive(new URI("/user/root/Projects/datafiles/RSJ/departments_map.tar.gz"), conf);
		
		// Set source index for the input files;
		// the following configuration can be done dynamically either with -D options or by providing configuration file.
		conf.setInt("part-e", 1);
		conf.setInt("part-sc", 2);
		conf.setInt("part-sh", 3);
		
		StringBuilder inputPaths = new StringBuilder();
		inputPaths.append(args[0])
				  .append(",")
				  .append(args[1]);
		
		
		FileInputFormat.setInputPaths(job, inputPaths.toString());
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(MapperRSJ.class);
		job.setMapOutputKeyClass(CompositeKeyWritableRSJ.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(PartitionerRSJ.class);
		job.setSortComparatorClass(SortingComparatorRSJ.class);
		job.setGroupingComparatorClass(GroupingComparatorRSJ.class);

		job.setNumReduceTasks(4);
		job.setReducerClass(ReducerRSJ.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0: 1;
	}

	
	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new DriverRSJ(), args));

	}

	
}
