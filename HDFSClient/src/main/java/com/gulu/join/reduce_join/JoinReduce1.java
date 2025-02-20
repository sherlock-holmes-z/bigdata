package com.gulu.join.reduce_join;

import com.gulu.join.TableBean;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Text;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/20 17:13
 */
public class JoinReduce1 extends Reducer<Text, TableBean,Text,TableBean> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, Text, TableBean>.Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
    }
}
