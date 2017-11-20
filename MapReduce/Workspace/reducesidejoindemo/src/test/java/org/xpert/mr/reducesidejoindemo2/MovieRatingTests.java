package org.xpert.mr.reducesidejoindemo2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;


public class MovieRatingTests {

	private MapDriver<LongWritable,Text,LongWritable,MovieRatingWritable> mapDriverMovie;
	private MapDriver<LongWritable, Text, LongWritable, MovieRatingWritable> mapDriverRating;
	
	
	@Before
	public void setUp(){
		Mapper<LongWritable, Text, LongWritable, MovieRatingWritable> mapperMovie = new MovieMapper();
		Mapper<LongWritable, Text, LongWritable, MovieRatingWritable> mapperRating = new RatingMapper();
		
		Reducer<LongWritable, MovieRatingWritable, LongWritable, Text> reducer = new MovieRatingReducer();
		
		mapDriverMovie = MapDriver.newMapDriver(mapperMovie);
		mapDriverRating = MapDriver.newMapDriver(mapperRating);
		
	}
	
	@Test
	public void TestMovieMapper1(){
		
		/*
		2::Jumanji (1995)::Adventure|Children's|Fantasy
		3::Grumpier Old Men (1995)::Comedy|Romance
		4::Waiting to Exhale (1995)::Comedy|Drama
		5::Father of the Bride Part II (1995)::Comedy
		*/
		
		mapDriverMovie.withInput(new LongWritable(1), new Text("1::Toy Story (1995)::Animation|Children's|Comedy"));
		
		try {
			List<Pair<LongWritable, MovieRatingWritable>> kv = mapDriverMovie.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
					  
	}
	
	@Test
	public void TestRatingMapper1(){
		
		mapDriverRating.withInput(new LongWritable(1), new Text("1::1193::5::978300760"));
		
		
		try {
			List<Pair<LongWritable, MovieRatingWritable>> kv = mapDriverRating.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
