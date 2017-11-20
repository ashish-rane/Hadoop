package com.nyse.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.nyse.keyvalues.LongPair;

/**
 * 
 * @author Ashish.Rane
 * Grouping Comparator does not change the sort order of the keys 
 * ( its decided either by the key class compareTo() function or by the specified sort comparator.
 * 
 *  this comparator is used to indicate which keys in the mapper output should be grouped together and
 *  sent to the reducer in a single call.
 *  
 *  NOTE: Without this comparator specified the grouping will by default also use the compareTo() function of the key class.
 *  By specifying this class we can override this default behavior by grouping on a part of the key then a entire key.
 *  
 *  In this case we are specifying to group by first field only (i.e. Date ). This will result in all the stock records on 
 *  a particular trade date will be grouped together and sent to the reducer's reduce() method in one single call.
 *  Without this each combination of Date and Volume will be sent to a separate reduce() method call, which will not allow us to pick top three
 *  since we will get only one record at a time or more if the volume of two or more stocks happens to be the same on the same date.
 */

public class LongPairGroupingComparator extends WritableComparator{

	
	protected LongPairGroupingComparator(){
		super(LongPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		LongPair first = (LongPair)a;
		LongPair second = (LongPair)b;
		
		// Use only the first part of the key (i.e the date ) to group 
		return LongPair.compare(first.getFirst(), second.getFirst());
	}
	
	
}
