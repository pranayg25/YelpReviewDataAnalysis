package com.neu;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Driver(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// Create configuration
		Configuration conf = new Configuration(true);

		//Creating Distributed Cache
		DistributedCache.addCacheFile(new URI("/home/pranay/Downloads/SentimentAnalysis-master/AFINN.txt"),conf);

		// Create job & Submitting job
		Job job = new Job(conf, "SentimentAnalysis");
		job.setJarByClass(Driver.class);

		// Setup MapReduce
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
