package com.yelp.analysis3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.jboss.netty.util.internal.StringUtil;

public class Mapper2 extends Mapper<Object, Text, Text, CompositeValueWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] fields = value.toString().split("\t");
			
			
			if(fields.length<3 || (fields[0].trim().equals("") || (fields[0]).equals("") || (fields[0]).isEmpty() || (fields[0])=="")  ||
					(fields[1].trim().equals("") || (fields[1]).equals("")|| (fields[1]).isEmpty() || (fields[1])=="") ||
					(fields[2].trim().equals("") || (fields[2]).equals("") || (fields[2]).isEmpty() || (fields[2])=="")) {
				System.out.println("gadbad");
			}
			int val = Integer.parseInt(fields[2]);
			context.write(new Text(fields[0]), new CompositeValueWritable(fields[1],fields[2]));
			
	}
}
