package com.gulu.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 前两个是map阶段输入的k-v，需要对应Mapper的输出类型
 * 后两个是输出的k-v
 * <p>
 * Reduce对每一组相同key的k-v组调用一次reduce方法
 *
 * @author Gollum
 * @date 2024-12-17 22:38
 */
public class PartitionReduce extends Reducer<Text, AreaBean, Text, IntWritable> {

    int sum;
    IntWritable v = new IntWritable();


    /**
     * @param key     mapper传过来的key
     * @param values  相同key的所有value值（相同key的k-v组调用一次reduce方法）
     * @param context 输出的k-sum统计结果
     */
    @Override
    protected void reduce(Text key, Iterable<AreaBean> values, Reducer<Text, AreaBean, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (AreaBean areaBean : values) {
            sum += areaBean.getNum();
        }
        // 2 输出
        v.set(sum);
        context.write(key, v);
    }
}
