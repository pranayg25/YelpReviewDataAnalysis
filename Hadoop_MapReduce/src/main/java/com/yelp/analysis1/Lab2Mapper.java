package com.yelp.analysis1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Lab2Mapper extends Mapper<Object, Text, CompositeKeyWritable, FloatWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		JSONObject json = null;
		try {
			json = new JSONObject(value.toString());
		} catch (JSONException e1) {
			e1.printStackTrace();
		}
		
		//System.out.println(value.toString());
		String city = null;
		String hood = null;
		try {
			city = json.getString("city");
			hood = json.getString("neighborhood");
			if(hood==null || hood.equals("") || hood.isEmpty()) {
				hood = "NA";
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

		if ((null != city && !city.isEmpty() && !city.equals("")) || null != hood) {

			CompositeKeyWritable cw = new CompositeKeyWritable(city, hood);

			try {
				context.write(cw, new FloatWritable((float) json.getDouble("stars")));
			} catch (Exception e) {
				System.out.println(cw);
				// System.out.println(values[10]);
				try {
					System.out.println(json.getString("city"));
				} catch (JSONException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				System.out.println("" + e.getMessage());

			}

		}

	}

}