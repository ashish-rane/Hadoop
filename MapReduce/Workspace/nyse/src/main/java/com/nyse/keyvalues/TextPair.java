package com.nyse.keyvalues;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

// Class which implements a custom key which is made up of 2 string parts
public class TextPair implements WritableComparable<TextPair> {
	

	// parts that make up the key.
	private Text first;
	private Text second;

	// Properties
	
	public Text getFirst() {
		return first;
	}

	public void setFirst(Text first) {
		this.first = first;
	}

	public Text getSecond() {
		return second;
	}

	public void setSecond(Text second) {
		this.second = second;
	}
	
	
	/* Constructors  */
	
	public TextPair()
	{
		super();
		first = new Text();
		second = new Text();
	}
	

	public TextPair(Text first, Text second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	public TextPair(String first, String second) {
		super();
		this.first = new Text(first);
		this.second = new Text(second);
	}
	
	// Overridden methods
	public void write(DataOutput out) throws IOException {
		
		this.first.write(out);
		this.second.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		
		this.first.readFields(in);
		this.second.readFields(in);
	}

	// Compare To function dictates how the keys are also sorted in the output.
	public int compareTo(TextPair o) {
		
		int result = this.first.compareTo(o.first);
		
		if(result != 0)
		{
			return result;
		}

		return this.second.compareTo(o.second);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		
		// Note : Calculating the hash code only on second field will result in
		// all same values of second field (Stock Ticker) will go to the same
		// partition and hence the same reducer. which means the output files will be
		// grouped together by Stock ticker.
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
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
		TextPair other = (TextPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return first + " " + second;
	}
	
	

}
