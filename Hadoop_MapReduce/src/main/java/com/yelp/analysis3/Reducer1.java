package com.yelp.analysis3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {

	public void reduce(CompositeKeyWritable key,Iterable<IntWritable> values,Context context)throws IOException, InterruptedException {
		int total = 0;
		for(IntWritable value :values){
			total += value.get();
		}
		context.write(key,new IntWritable(total));
	}
}
