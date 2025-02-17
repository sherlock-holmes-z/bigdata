package com.gulu.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, map阶段输入的content的偏移量
 * VALUEIN, map阶段输入的content
 * <p>
 * KEYOUT, map阶段输出的key
 * VALUEOUT, map阶段输出的key的value值
 * <p>
 * Mapper对每一个k-v调用一次
 *
 * @author Gollum
 * @date 2024-12-17 22:37
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, AreaBean> {


    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, AreaBean>.Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        String area = words[0];
        Integer num = Integer.valueOf(words[1]);

        // 3 输出
        k.set(area);
        context.write(k, new AreaBean(area, num));
    }
}
