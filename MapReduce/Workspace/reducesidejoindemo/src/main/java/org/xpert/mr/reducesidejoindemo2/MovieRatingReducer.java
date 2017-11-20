package org.xpert.mr.reducesidejoindemo2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<LongWritable, MovieRatingWritable, LongWritable, Text>{

	private StringBuilder reduceValueBuilder = new StringBuilder();
	
	@Override
	protected void reduce(
			LongWritable key,
			Iterable<MovieRatingWritable> values,
			Reducer<LongWritable, MovieRatingWritable, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

		int sumRatings = 0;
		int count = 0;
		
		System.out.println("Movie ID: " + key.toString());	
		
		for(MovieRatingWritable val : values){
			
			// if its a movie append movie name
			if(val.isMovie()){
				reduceValueBuilder.append(val.getMovieName())
								  .append(",");
			}
			else
			{
				// for rating accumulate the rating and the number of ratings to get the average
				sumRatings += val.getRating();
				count++;
			}
		}
		
		if(count == 0){
			reduceValueBuilder.append("NOT RATED!!!");
			context.write(key, new Text(reduceValueBuilder.toString()));
			
			reduceValueBuilder.setLength(0);
			return;
		}
		
		// calculate average rating and write the sentiment
		int avgRating = sumRatings/count;
		String sentiment = avgRating > 3 ? "POSITIVE" : (avgRating == 3 ? "AVERAGE" : "NEGATIVE");
		
		
		reduceValueBuilder.append(avgRating).append(",").append(sentiment);
		
		context.write(key, new Text(reduceValueBuilder.toString()));
		
		reduceValueBuilder.setLength(0);
	}

	
	
	
}
