package com.yelp.analysis4;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Mapper1 extends Mapper<Object, Text, CompositeKeyWritable, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			JSONObject	json = new JSONObject(value.toString());
			JSONArray checkins = json.getJSONArray("time");
			for(int i=0;i<checkins.length();i++) {
				String[] parts = ((String)checkins.get(i)).split("-|\\:");
				context.write(new CompositeKeyWritable(json.getString("business_id"),parts[0]), new IntWritable(Integer.parseInt(parts[2])));
			}

		} catch (JSONException e1) {
			e1.printStackTrace();
		}

	}

}