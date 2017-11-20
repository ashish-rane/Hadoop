package org.xpert.mr.reducesidejoindemo2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Ashish.Rane
 * Making the input key to the mapper as IntWritable results in error. 
 */
public class MovieMapper extends Mapper<LongWritable, Text, LongWritable, MovieRatingWritable>{

	private MovieRatingWritable mapOutputValue = new MovieRatingWritable();
	private LongWritable mapOutputKey = new LongWritable();
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, LongWritable, MovieRatingWritable>.Context context)
			throws IOException, InterruptedException {
		
		try{
			String[] fields = value.toString().split("::");
			
			mapOutputKey.set(Long.parseLong(fields[0]));
			
			mapOutputValue.setIsMovie(true);
			mapOutputValue.setMovieName(fields[1]);
			
			
		}
		catch(Exception ex){
			System.err.println("Incorrect Record: " + value.toString());
			return;
		}
		
		context.write(mapOutputKey, mapOutputValue);
	}
	
	

}
