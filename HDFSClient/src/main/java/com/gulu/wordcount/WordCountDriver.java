package com.gulu.wordcount;

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
     * 可打包至服务器的jar
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 获取配置信息及job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 设置driver的jar
        job.setJarByClass(WordCountDriver.class);

        // 设置mapper的jar及输出k-v类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce的jar及输出k-v类型
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }

    /**
     * 在本地执行hadoop任务
     */
//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        // 获取配置信息及job对象
//        Configuration configuration = new Configuration();
//        Job job = Job.getInstance(configuration);
//
//        job.setJarByClass(WordCountDriver.class);
//
//        job.setMapperClass(WordCountMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        job.setReducerClass(WordCountReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        FileInputFormat.setInputPaths(job, new Path("D:\\test\\hadoop\\input\\1.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("D:\\test\\hadoop\\output\\1.txt"));
//
//        boolean b = job.waitForCompletion(true);
//        System.exit(b ? 0 : 1);
//    }


    /**
     * 在本地调用集群，执行hadoop任务
     */
//    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        // 获取配置信息及job对象
//        Configuration configuration = new Configuration();
//        //设置在集群运行的相关参数-设置HDFS,NAMENODE的地址
//        configuration.set("fs.defaultFS", "hdfs://hadoop202:8020");
//        //指定MR运行在Yarn上
//        configuration.set("mapreduce.framework.name", "yarn");
//        //指定MR可以在远程集群运行
//        configuration.set("mapreduce.app-submission.cross-platform", "true");
//        //指定yarn resourcemanager的位置
//        configuration.set("yarn.resourcemanager.hostname", "hadoop203");
//
//        Job job = Job.getInstance(configuration);
//
//        // 设置driver的jar为本地jar包
//        job.setJar("D:\\code\\bigdata\\HDFSClient\\target\\HDFSClient-1.0-SNAPSHOT.jar");
//
//        // 设置mapper的jar及输出k-v类型
//        job.setMapperClass(WordCountMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//
//        // 设置reduce的jar及输出k-v类型
//        job.setReducerClass(WordCountReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//
//        // 设置输入和输出路径
//        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop202:8020/wcinput/word.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop202:8020/output2"));
//
//        // 提交job
//        boolean b = job.waitForCompletion(true);
//        System.exit(b ? 0 : 1);
//    }


}
