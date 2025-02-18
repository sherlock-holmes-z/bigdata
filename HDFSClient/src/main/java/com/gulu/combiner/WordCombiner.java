package com.gulu.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/18 17:01
 */
public class WordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable v = new IntWritable(0);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
