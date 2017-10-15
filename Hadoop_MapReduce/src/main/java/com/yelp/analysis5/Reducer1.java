package com.yelp.analysis5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends
Reducer<Text, IntWritable, Text , IntWritable> {



	public void reduce(Text key,Iterable<IntWritable> values,Context context)
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

		context.write(key,new IntWritable(total));
	}
}