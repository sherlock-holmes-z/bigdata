package com.gulu.join.reduce_join;

import com.gulu.join.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/20 17:14
 */
public class JoinMapper1 extends Mapper<LongWritable, Text, Text, TableBean> {

    Text k = new Text();
    TableBean v = new TableBean();
    private String fileName;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        this.fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        if (fileName.startsWith("order")) {
            k.set(split[1]);
        } else {
            k.set(split[0]);
        }
        context.write(k, v);
    }
}
