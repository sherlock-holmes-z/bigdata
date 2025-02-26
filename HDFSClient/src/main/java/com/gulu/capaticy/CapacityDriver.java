package com.gulu.capaticy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/26 17:13
 */
public class CapacityDriver {

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        // 设置提交的队列
        configuration.set("mapreduce.job.queuename","hive");

        Job job = Job.getInstance(configuration);

        // 设置任务优先级，值越低优先级越高
        job.setPriorityAsInteger(1);

    }
}
