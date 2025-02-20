package com.gulu.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Gollum
 * @date 2024-12-17 22:38
 */
public class WordCountDriver {

    /**
     * 在本地执行hadoop任务
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 获取配置信息及job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置combiner，在mapTask中提前聚合
        job.setCombinerClass(WordCombiner.class);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/combiner/combiner.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/combiner/out1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
