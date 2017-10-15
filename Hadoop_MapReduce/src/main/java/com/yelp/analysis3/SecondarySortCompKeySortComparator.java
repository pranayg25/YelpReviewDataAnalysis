package com.yelp.analysis3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {

	protected SecondarySortCompKeySortComparator() {
		super(CompositeValueWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeValueWritable key1 = (CompositeValueWritable) w1;
		CompositeValueWritable key2 = (CompositeValueWritable) w2;

		int cmpResult = key1.getCity().compareTo(key2.getCity());
		if (cmpResult == 0)// same deptNo
		{
			return -key1.getCount().compareTo(key2.getCount());
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}
}