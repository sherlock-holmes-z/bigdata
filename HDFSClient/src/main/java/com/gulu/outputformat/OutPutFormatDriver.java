package com.gulu.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 自定义数据输出
 *
 * @author Gollum
 * @date 2025-02-18 22:45
 */
public class OutPutFormatDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OutPutFormatDriver.class);

        job.setMapperClass(OutPutFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OutPutFormatReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置自定的outputFormat
        job.setOutputFormatClass(LogOutPutFormat.class);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/outputformat/1.txt"));
        // output需要输出一个success文件
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/outputformat/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
