package com.yelp.analysis3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, CompositeValueWritable, Text, CompositeValueWritable> {

	public void reduce(Text key,Iterable<CompositeValueWritable> values,Context context)throws IOException, InterruptedException {
		//int total = 0;
		int i=1;
		for(CompositeValueWritable value :values){
		//	total += value.get();
			context.write(key,value);
			if(i==5) {
				break;
			}
			i++;
		}
		
	}
}
