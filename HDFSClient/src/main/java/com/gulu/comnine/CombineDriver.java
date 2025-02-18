package com.gulu.comnine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

import java.io.IOException;

/**
 * 切片机制
 *
 * @author chocolate
 * 2025/2/17 15:13
 */
public class CombineDriver {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 用于小文件过多的场景，将多个小文件交给一个mapTask处理
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);

    }
}
