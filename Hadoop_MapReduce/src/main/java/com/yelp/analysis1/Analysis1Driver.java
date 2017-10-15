package com.yelp.analysis1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Analysis1Driver {

	public static void main(String[] args) throws Exception {

		Path inputPath = new Path("/home/pranay/Downloads/yelp_dataset_challenge_round9/us_business.json");
		Path outputDir = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis1_output");

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "Analysis 1");
		job.setJarByClass(Lab2Mapper.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
	//	job.setPartitionerClass(NaturalKeyPartitioner.class);
		job.setSortComparatorClass(SecondarySortCompKeySortComparator.class);

		// Setup MapReduce
		job.setMapperClass(Lab2Mapper.class);
		job.setReducerClass(Lab2Reducer.class);
//		job.setNumReduceTasks(2);

		// Specify key / value
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(FloatWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

}