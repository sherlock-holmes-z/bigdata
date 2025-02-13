package com.gulu.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/13 16:07
 */
public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {


    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        FlowBean flowBeanSum = new FlowBean();
        for (FlowBean flowBean : values) {
            flowBeanSum.setUpFlow(flowBeanSum.getUpFlow() + flowBean.getUpFlow());
            flowBeanSum.setDownFlow(flowBeanSum.getDownFlow() + flowBean.getDownFlow());
        }

        flowBeanSum.setSumFlow(flowBeanSum.getUpFlow() + flowBeanSum.getDownFlow());
        context.write(key, flowBeanSum);

    }
}
