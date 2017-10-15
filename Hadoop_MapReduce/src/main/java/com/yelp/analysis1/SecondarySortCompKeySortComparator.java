package com.yelp.analysis1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {

	protected SecondarySortCompKeySortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getCity().compareTo(key2.getCity());
		if (cmpResult == 0)// same deptNo
		{
			return -key1.getHood().compareTo(key2.getHood());
			//If the minus is taken out, the values will be in
			//ascending order
		}
		return cmpResult;
	}
}