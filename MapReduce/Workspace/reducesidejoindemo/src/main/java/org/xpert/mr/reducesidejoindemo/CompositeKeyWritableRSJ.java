package org.xpert.mr.reducesidejoindemo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritableRSJ implements WritableComparable<CompositeKeyWritableRSJ>{
	
	private String joinKey; // Employee ID
	private int srcIndex; // 1: Employee, 2:Salary (Current) data, 3: Salary (historical) data
	
	
	public String getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String joinKey) {
		this.joinKey = joinKey;
	}

	public int getSrcIndex() {
		return srcIndex;
	}

	public void setSrcIndex(int srcIndex) {
		this.srcIndex = srcIndex;
	}

	public CompositeKeyWritableRSJ(){
		
	}

	@Override
	public String toString() {
		return (new StringBuilder().append(joinKey).append("\t").append(srcIndex)).toString();
	}

	public CompositeKeyWritableRSJ(String joinKey, int srcIndex) {
		super();
		this.joinKey = joinKey;
		this.srcIndex = srcIndex;
	}

	public void write(DataOutput out) throws IOException {
		
		WritableUtils.writeString(out, joinKey);
		WritableUtils.writeVInt(out, srcIndex);
		
	}

	public void readFields(DataInput in) throws IOException {
		
		joinKey = WritableUtils.readString(in);
		srcIndex = WritableUtils.readVInt(in);
		
	}

	public int compareTo(CompositeKeyWritableRSJ o) {
		
		int result = joinKey.compareTo(o.joinKey);
		
		if(result == 0){
			result = Integer.compare(srcIndex, o.srcIndex);
		}
		
		return result;
	}

}
