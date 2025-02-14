package com.gulu.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author chocolate
 * 2025/2/13 16:07
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();

    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String str = value.toString();

        String[] split = str.split(" ");

        k.set(split[1]);

        flowBean.setUpFlow(Long.parseLong(split[2]));
        flowBean.setDownFlow(Long.parseLong(split[3]));
        context.write(k, flowBean);
    }

}
