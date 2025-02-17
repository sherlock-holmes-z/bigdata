package com.gulu.partition;

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
public class PartitionDriver {

    /**
     * 在本地执行hadoop任务
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 获取配置信息及job对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PartitionDriver.class);

        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AreaBean.class);

        job.setReducerClass(PartitionReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AreaBean.class);

        // 设置自定义partition
        job.setPartitionerClass(MyPartition.class);
        // 设置的reduce数量不能低于partition里的分区量，否则分区完之后，多余的分区不知道该去哪个reduce处理
        // 没有处理partition的reduce会输出一个空文件，reduceTask只能多不能少
        job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/partition/partition1.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/partition/out1"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }


}
