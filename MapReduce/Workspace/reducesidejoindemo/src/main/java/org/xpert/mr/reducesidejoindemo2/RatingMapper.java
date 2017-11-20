package org.xpert.mr.reducesidejoindemo2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingMapper extends Mapper<LongWritable, Text, LongWritable, MovieRatingWritable> {

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
			
			mapOutputKey.set(Long.parseLong(fields[1]));
			
			mapOutputValue.setIsMovie(false);
			mapOutputValue.setRating(Float.parseFloat(fields[2]));
		}
		catch(Exception ex){
			System.err.println("Incorrect Record: " + value.toString());
			return;
		}
		
		context.write(mapOutputKey, mapOutputValue);
		
	}

	
	
}
