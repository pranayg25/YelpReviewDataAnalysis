package com.yelp.analysis7.part2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] parts = value.toString().split("\t");
		Text business_id = new Text(parts[0]);
		float actual = Float.parseFloat(parts[1]);
		float senti = Float.parseFloat(parts[3]);
		float diff = actual-senti;
		if(diff >= -0.5 && diff <=0.5) {
			context.write(new Text("actaul ~ sa"), new IntWritable(1));	
		}else if(diff>=1) {
			context.write(new Text("actual > sa"), new IntWritable(1));
		}else if(diff<=1) {
			context.write(new Text("actual < sa"), new IntWritable(1));
		}else {
			System.out.println("gafla");
		}
	}
}