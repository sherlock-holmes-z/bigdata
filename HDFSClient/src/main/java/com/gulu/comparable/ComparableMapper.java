package com.gulu.comparable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/18 15:25
 */
public class ComparableMapper extends Mapper<LongWritable, Text, PersonBean, LongWritable> {

    PersonBean personBean = new PersonBean();

    LongWritable v = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PersonBean, LongWritable>.Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] split = string.split(" ");

        personBean.setArea(split[0]);
        personBean.setAge(Integer.parseInt(split[1]));
        personBean.setCount(Integer.parseInt(split[2]));

        v.set((long) personBean.getAge() * personBean.getCount());
        context.write(personBean, v);
    }
}
