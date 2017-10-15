package com.neu;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




public class MapClass extends Mapper<Object, Text, Text, FloatWritable>{

	private URI[] files;
	private HashMap<String, String> AFINN_map = new HashMap<String, String>();

	
	public void setup(Context context) throws IOException
	{
		files = DistributedCache.getCacheFiles(context.getConfiguration());
		//System.out.println("files:"+ files);
		Path path = new Path(files[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line="";

		while((line = br.readLine()) != null)
		{
			String splits[] = line.split("\t");
			AFINN_map.put(splits[0], splits[1]);
		}
		br.close();
		in.close();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		JSONParser jp = new JSONParser();
		try {
			
			JSONObject obj = (JSONObject) jp.parse(value.toString());
			if(obj.containsKey("business_id") && obj.containsKey("text"))
			{
				String review = obj.get("text").toString();
				//System.out.println("Map:Serving: "+ review);
				int sentiment_sum = 0;
				String[] words = review.split(" ");
				int count=1;
				for(String word : words)
				{
					if(word.length() > 1 && AFINN_map.containsKey(word))
					{
						Integer s = new Integer(AFINN_map.get(word));
						sentiment_sum += s;
						count++;
					}
				}
				String aboutProduct = obj.get("business_id").toString();
				float average = count<=1?(sentiment_sum/count):(sentiment_sum/(count-1));
				context.write(new Text(aboutProduct), new FloatWritable(average));
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}
		

	}
	
}
