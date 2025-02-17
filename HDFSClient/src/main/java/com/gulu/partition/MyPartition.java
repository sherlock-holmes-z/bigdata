package com.gulu.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 第一个参数是mapper输出的k。第二个输出的v
 *
 * @author chocolate
 * 2025/2/17 16:56
 */
public class MyPartition extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return 0;
    }
}
