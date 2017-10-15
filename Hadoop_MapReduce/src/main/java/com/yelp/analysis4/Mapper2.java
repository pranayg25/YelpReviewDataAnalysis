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

public class Mapper2 extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			JSONObject	json = new JSONObject(value.toString());
			context.write(new Text(json.getString("business_id")), new Text(json.getString("city")));

		} catch (JSONException e1) {
			e1.printStackTrace();
		}

	}

}