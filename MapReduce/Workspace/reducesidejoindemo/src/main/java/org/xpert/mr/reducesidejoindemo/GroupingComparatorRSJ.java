package org.xpert.mr.reducesidejoindemo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorRSJ extends WritableComparator {

	
	protected GroupingComparatorRSJ(){
		super(CompositeKeyWritableRSJ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		// The grouping comparator groups by join key
		CompositeKeyWritableRSJ key1 = (CompositeKeyWritableRSJ)a;
		CompositeKeyWritableRSJ key2 = (CompositeKeyWritableRSJ)b;
		
		return key1.getJoinKey().compareTo(key2.getJoinKey());
	}
	
	
}
