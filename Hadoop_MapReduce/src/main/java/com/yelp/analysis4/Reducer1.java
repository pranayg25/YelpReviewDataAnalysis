package com.yelp.analysis4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends
Reducer<CompositeKeyWritable, IntWritable, CompositeKeyWritable, IntWritable> {



	public void reduce(CompositeKeyWritable key,Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {

		int total = 0;
		//float average = 0;
		//int count = 0;
		for(IntWritable value :values){
			try {
				total += value.get();
				//count ++;
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		//average = total/count;

		context.write(key, new IntWritable(total));
	}
}