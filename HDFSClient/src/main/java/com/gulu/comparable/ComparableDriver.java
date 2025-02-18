package com.gulu.comparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/18 15:25
 */
public class ComparableDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(ComparableDriver.class);

        job.setMapperClass(ComparableMapper.class);
        job.setMapOutputKeyClass(PersonBean.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(ComparableReduce.class);
        job.setOutputKeyClass(PersonBean.class);
        job.setOutputValueClass(LongWritable.class);

        job.setPartitionerClass(ComparablePartition.class);
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/comparable/comparable.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/comparable/out1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
