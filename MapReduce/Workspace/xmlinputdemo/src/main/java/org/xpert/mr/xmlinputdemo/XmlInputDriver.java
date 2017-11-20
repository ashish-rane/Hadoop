package org.xpert.mr.xmlinputdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XmlInputDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		
		conf.set("xmlinput.start", "<property>");
		conf.set("xmlinput.end", "</property>");
		
		job.setJarByClass(XmlInputDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(XmlInputMapper.class);
		
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0: 1;
	}

	
	public static void main(String[] args) throws Exception{
		
		System.exit(ToolRunner.run(new XmlInputDriver(), args));
	}
		
	
}
