package org.xpert.mr.reducesidejoindemo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparatorRSJ extends WritableComparator{

	protected SortingComparatorRSJ() {
		
		super(CompositeKeyWritableRSJ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		// Sort on all the attributes of the composite key
		CompositeKeyWritableRSJ key1 = (CompositeKeyWritableRSJ)a;
		CompositeKeyWritableRSJ key2 = (CompositeKeyWritableRSJ)b;
		
		int cmpResult = key1.getJoinKey().compareTo(key2.getJoinKey());
		
		if(cmpResult == 0){
			cmpResult = Integer.compare(key1.getSrcIndex(), key2.getSrcIndex());
		}
		
		return cmpResult;
	}
	
	

	
}
