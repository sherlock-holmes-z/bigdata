package com.gulu.join.map_join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author chocolate
 * 2025/2/21 14:39
 */
public class JoinDriver2 {

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(JoinDriver2.class);

        job.setMapperClass(JoinMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0); // 不需要reduce阶段，设置reduce数量为0,此时输出文件根据mapper数量决定

        job.setCacheFiles(new URI[]{new URI("src/main/resources/join/pd.txt")});

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);

        FileInputFormat.setInputPaths(job
                , new Path("src/main/resources/join/order.txt")
                , new Path("src/main/resources/join/order2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/join/output1"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
