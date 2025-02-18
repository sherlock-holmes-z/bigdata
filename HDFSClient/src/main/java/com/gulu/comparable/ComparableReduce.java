package com.gulu.comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/18 15:25
 */
public class ComparableReduce extends Reducer<PersonBean, LongWritable, PersonBean, LongWritable> {

    long sumAge = 0;

    LongWritable v = new LongWritable();

    @Override
    protected void reduce(PersonBean key, Iterable<LongWritable> values, Reducer<PersonBean, LongWritable, PersonBean, LongWritable>.Context context) throws IOException, InterruptedException {

        for (LongWritable value : values) {
            sumAge += value.get();
        }
        v.set(sumAge);
        context.write(key, v);
    }
}
