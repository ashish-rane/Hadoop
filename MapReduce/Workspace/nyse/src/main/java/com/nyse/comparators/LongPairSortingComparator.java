package com.nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.nyse.keyvalues.LongPair;

/**
 * 
 * @author Ashish.Rane
 * This comparator is specified as sort comparator which will decide the sorting order within a 
 * group. This method is used to override the default sort order specified by the Key class compareTo() method.
 * 
 * Sorting will happen with keys within individual partitioner before the values belonging to same keys are grouped
 * together and sent to the reduce method call.
 */

public class LongPairSortingComparator extends WritableComparator {

	public LongPairSortingComparator() {
		super(LongPair.class, true);
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
	
		LongPair lp1 = (LongPair)a;
		LongPair lp2 = (LongPair)b;
		
		int cmp = LongPair.compare(lp1.getFirst(), lp2.getFirst());
		
		if(cmp != 0)
		{
			return cmp;
		}
		
		// - sign because we want in descending order
		return -LongPair.compare(lp1.getSecond(), lp2.getSecond());
	}

	
	
}
