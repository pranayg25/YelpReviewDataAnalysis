package com.yelp.analysis2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Analysis2Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Path inputPath1 = new Path("/home/pranay/Downloads/yelp_dataset_challenge_round9/us_business.json");
		Path outputPath1 = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis2_output");
		//Path inputPath2 = outputPath1;
		//Path outputPath2 = new Path("/home/pranay/Downloads/SecondarySortingLab-master/analysis2_2_output");


		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "Analysis 2");
		job.setJarByClass(Mapper1.class);

		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputPath1);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputPath1);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath1))
			hdfs.delete(outputPath1, true);
		/*if (hdfs.exists(outputPath2))
			hdfs.delete(outputPath2, true);*/

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
