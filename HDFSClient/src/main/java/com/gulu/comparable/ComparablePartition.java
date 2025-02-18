package com.gulu.comparable;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author chocolate
 * 2025/2/18 16:09
 */
public class ComparablePartition extends Partitioner<PersonBean, LongWritable> {

    @Override
    public int getPartition(PersonBean personBean, LongWritable longWritable, int i) {
        if ("shanghai".equals(personBean.getArea())){
            return 0;
        }else {
            return 1;
        }
    }
}
