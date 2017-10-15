package com.yelp.analysis7.part2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;

public class Analysis7Driver {

	public static void main(String[] args) throws Exception {

		Path inputPath = new Path("/home/pranay/Downloads/pig/Analysis7/part-r-00000");
		Path outputDir = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis7_2_output");
		/*Path inputPath1 = new Path("/home/pranay/Downloads/yelp_dataset_challenge_round9/yelp_academic_dataset_business.json");
		Path outputDir1 = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis4_output1");
*/
		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "Analysis 7");
		job.setJarByClass(Mapper1.class);
//		job.setGroupingComparatorClass(GroupingComparator.class);
		//	job.setPartitionerClass(NaturalKeyPartitioner.class);
		//job.setSortComparatorClass(SecondarySortCompKeySortComparator.class);

		// Setup MapReduce
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		//		job.setNumReduceTasks(2);

		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
/*		if (hdfs.exists(outputDir1))
			hdfs.delete(outputDir1, true);*/
		
		// Execute job
		boolean code = job.waitForCompletion(true);
		System.exit(code ? 0 : 1);

		/*if(code) {
			// Create configuration
			Configuration conf1 = new Configuration(true);

			// Create job
			Job job1 = new Job(conf1, "Analysis 4");
			job1.setJarByClass(Mapper2.class);

			// Setup MapReduce
			job1.setMapperClass(Mapper2.class);
			job1.setReducerClass(Reducer2.class);
			//		job.setNumReduceTasks(2);

			// Specify key / value
			job1.setMapOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);

			// Input
			FileInputFormat.addInputPath(job1, inputPath1);
			job1.setInputFormatClass(TextInputFormat.class);

			// Output
			FileOutputFormat.setOutputPath(job1, outputDir1);

			boolean code1 = job1.waitForCompletion(true);
			System.exit(code1 ? 0 : 1);
		}*/
	}

}