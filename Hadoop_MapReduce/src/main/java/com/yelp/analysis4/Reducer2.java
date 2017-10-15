package com.yelp.analysis4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends
Reducer<Text, Text, Text, Text> {



	public void reduce(Text key,Iterable<Text> values,Context context)
			throws IOException, InterruptedException {

		for(Text value :values){
			context.write(key, value);
		}
	}
}