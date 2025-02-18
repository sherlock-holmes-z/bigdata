package com.gulu.comnine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 切片机制
 *
 * @author chocolate
 * 2025/2/17 15:13
 */
public class CombineDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(CombineDriver.class);

        job.setMapperClass(CombineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(CombineReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/combine/1.txt"),new Path("src/main/resources/combine/2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/combine/output2"));

        // 用于小文件过多的场景，将多个小文件交给一个mapTask处理
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
