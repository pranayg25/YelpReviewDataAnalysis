package com.neu;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends
	Reducer<Text, FloatWritable, Text, FloatWritable>{

	public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException
	{
		float sum = 0;
		float average = 0;
		int count = 1;
		for(FloatWritable val : value){
			sum += val.get();
			count++;
		}
		average = (count<1?(sum/count):(sum/(count-1)))+3;
		average = (float) (Math.round(average * 2) / 2.0);
		context.write(key, new FloatWritable(average));
	}
}
