package com.nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

//Class which implements a custom key OR value which is made up of 2 numeric parts
public class LongPair implements WritableComparable<LongPair> {
	

	// parts that make up the key.
	private long first;
	private long second;

	// Properties
	
	public long getFirst() {
		return first;
	}

	public void setFirst(long first) {
		this.first = first;
	}

	public long getSecond() {
		return second;
	}

	public void setSecond(long second) {
		this.second = second;
	}
	
	
	/* Constructors  */
	
	public LongPair()
	{
		super();
	}
	

	
	
	public LongPair(Long first, Long second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	// Overridden methods
	public void write(DataOutput out) throws IOException {
		
		out.writeLong(first);
		out.writeLong(second);;
	}

	public void readFields(DataInput in) throws IOException {
		
		first = in.readLong();
		second = in.readLong();
	}

	/**
	 * As far as possible keep the members of keys as primitive types otherwise there will
	 * be performance lost in serialization/deserialization during sort/shuffle.
	 */
	public int compareTo(LongPair o) {
		
		int result = compare(first, o.first);
		if(result != 0)
		{
			return result;
		}

		
		return compare(second, o.second);
	}

	/**
	 * Convenience method for comparing two longs
	 * @see java.lang.Object#hashCode()
	 */
	public static int compare(long a, long b){
		return a < b ? -1: (a > b ? 1: 0);
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (first ^ (first >>> 32));
		result = prime * result + (int) (second ^ (second >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LongPair other = (LongPair) obj;
		if (first != other.first)
			return false;
		if (second != other.second)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return first + " " + second;
	}
	
	

}

