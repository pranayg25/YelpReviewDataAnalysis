package com.yelp.analysis3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Analysis3Driver{

	public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException {

		Path inputPath1 = new Path("/home/pranay/Downloads/yelp_dataset_challenge_round9/us_business.json");
		Path outputPath1 = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis3_output");
		/*Path inputPath2 = outputPath1;
		Path outputPath2 = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis3_output2");*/

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "Analysis 3");
		job.setJarByClass(Mapper1.class);

		// Setup MapReduce
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		//		job.setNumReduceTasks(2);

		// Specify key / value
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath1);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputPath1);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		/*if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);
*/
		boolean isComplete = job.waitForCompletion(true);
		System.exit(isComplete?0:1);
		//		System.exit(job.waitForCompletion(true) ? 0 : 1);
/*		if(isComplete) {
			Configuration conf2 = new Configuration(true);

			// Create job
			Job job2 = new Job(conf2, "Analysis 3 -2");
			job2.setJarByClass(Mapper2.class);

			job2.setMapperClass(Mapper2.class);
			job2.setReducerClass(Reducer2.class);

			//job.setGroupingComparatorClass(GroupingComparator.class);
			job2.setSortComparatorClass(SecondarySortCompKeySortComparator.class);

			// Specify key / value
			job2.setMapOutputKeyClass(Text.class);
			job2.setOutputValueClass(CompositeValueWritable.class);
			job2.setOutputKeyClass(Text.class);


			FileInputFormat.addInputPath(job2, inputPath2);
			job2.setInputFormatClass(TextInputFormat.class);

			// Output
			FileOutputFormat.setOutputPath(job2, outputPath2);
			job2.setOutputFormatClass(TextOutputFormat.class);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);			
		}*/
	}
}