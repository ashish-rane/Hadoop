package org.xpert.mr.reducesidejoindemo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MovieRatingDriver extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		
		if(args.length != 3){
			System.out.println("Requires 3 arguments <inputDir1> <inputDir2> <outputDir>");
		}
		
		Job job = Job.getInstance(getConf());
		Configuration conf = job.getConfiguration();
		job.setJarByClass(MovieRatingDriver.class);
		job.setJobName("Movie Ratings Sentiment");
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// This is how can add multiple mappers with the corresponding input paths
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MovieRatingWritable.class);
		
		job.setReducerClass(MovieRatingReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		

		return job.waitForCompletion(true) ? 0: 1;
	}

	
	public static void main(String[] args) throws Exception {

		System.exit(ToolRunner.run(new MovieRatingDriver(), args));

	}
}
