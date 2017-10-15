package com.yelp.analysis1;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, FloatWritable >
{
@Override
public int getPartition(CompositeKeyWritable key, FloatWritable value, int numReduceTasks) {
int hash = key.getCity().hashCode();
int partition = hash % numReduceTasks;
return partition;
}
}
