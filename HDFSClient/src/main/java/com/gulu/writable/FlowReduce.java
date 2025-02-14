package com.gulu.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/13 16:07
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBeanSum = new FlowBean();


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        long upFlow = 0, downFlow = 0;
        for (FlowBean flowBean : values) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
        }

        flowBeanSum.setUpFlow(upFlow);
        flowBeanSum.setDownFlow(downFlow);
        flowBeanSum.setSumFlow(upFlow + downFlow);
        context.write(key, flowBeanSum);
    }
}
