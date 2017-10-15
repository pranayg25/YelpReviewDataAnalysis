package com.yelp.analysis5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			JSONObject	json = new JSONObject(value.toString());
			if(json.get("hours").getClass()==JSONArray.class){
				JSONArray checkins = json.getJSONArray("hours");
				for(int i=0;i<checkins.length();i++) {
					String[] parts = ((String)checkins.get(i)).split("\\s|-");
					String[] startTime = parts[1].split(":");
					String[] endTime = parts[2].split(":");
					if(((Integer.parseInt(endTime[0])>22 && Integer.parseInt(endTime[0])<=23) || (Integer.parseInt(endTime[0])>=0 && Integer.parseInt(endTime[0])<=4)) && json.getInt("is_open")==1) {
						context.write(new Text(json.getString("city")), new IntWritable(1));
					}
				}	
			}

		} catch (JSONException e1) {
			e1.printStackTrace();
		}

	}

}