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
public class MyPartition extends Partitioner<Text, AreaBean> {

    @Override
    public int getPartition(Text text, AreaBean areaBean, int i) {
        String area = text.toString();
        if ("北京".equals(area)) {
            return 0;
        } else if ("上海".equals(area)) {
            return 1;
        } else if ("广州".equals(area)) {
            return 2;
        } else if ("深圳".equals(area)) {
            return 3;
        }
        return 4;
    }
}
