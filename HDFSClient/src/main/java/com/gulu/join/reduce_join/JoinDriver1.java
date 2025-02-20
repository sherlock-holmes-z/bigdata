package com.gulu.join.reduce_join;

import com.gulu.join.TableBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/20 17:15
 */
public class JoinDriver1 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(JoinDriver1.class);

        job.setMapperClass(JoinMapper1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setReducerClass(JoinReduce1.class);
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("src/main/resources/join/order.txt"), new Path("src/main/resources/join/pd.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/join/reduce_output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
