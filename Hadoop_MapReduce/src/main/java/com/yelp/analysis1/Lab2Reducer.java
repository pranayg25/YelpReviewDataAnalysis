package com.yelp.analysis1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Lab2Reducer extends
Reducer<CompositeKeyWritable, FloatWritable, CompositeKeyWritable, FloatWritable> {



	public void reduce(CompositeKeyWritable key,Iterable<FloatWritable> values,Context context)
			throws IOException, InterruptedException {

		float total = 0;
		float average = 0;
		int count = 0;
		for(FloatWritable value :values){
			try {
				total += value.get();
				count ++;
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		average = total/count;

		context.write(key, new FloatWritable(average));
	}
}