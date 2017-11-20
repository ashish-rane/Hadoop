package org.xpert.mr.reducesidejoindemo2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MovieRatingWritable implements Writable{

	private String movieName;
	private float rating;
	
	private boolean isMovie;
	
	
	
	public boolean isMovie() {
		return isMovie;
	}

	public void setIsMovie(boolean isMovie) {
		this.isMovie = isMovie;
	}

	public String getMovieName() {
		return movieName;
	}

	public void setMovieName(String movieName) {
		this.movieName = movieName;
	}

	public float getRating() {
		return rating;
	}

	public void setRating(float rating) {
		this.rating = rating;
	}
	
	public MovieRatingWritable(){
		
	}
	
	public MovieRatingWritable(String movieName, float rating){
		this.movieName = movieName;
		this.rating = rating;
	}

	public void write(DataOutput out) throws IOException {
		
		out.writeBoolean(isMovie);
		WritableUtils.writeString(out, movieName);
		out.writeFloat(rating);
	}

	public void readFields(DataInput in) throws IOException {
		
		isMovie = in.readBoolean();
		movieName = WritableUtils.readString(in);
		rating = in.readFloat();
		
	}

}
