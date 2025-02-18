package com.gulu.comnine;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, map阶段输入的content的偏移量
 * VALUEIN, map阶段输入的content
 *
 * KEYOUT, map阶段输出的key
 * VALUEOUT, map阶段输出的key的value值
 *
 * Mapper对每一个k-v调用一次
 *
 * @author Gollum
 * @date 2024-12-17 22:37
 */
public class CombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        // 1 获取一行
        String line = value.toString();

        // 2 切割
        String[] words = line.split(" ");

        // 3 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
