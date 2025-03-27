package com.gulu.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author chocolate
 * 2025/3/27 15:01
 */
public class Spark02_RDD_Memory {
    public static void main(String[] args) {

        // 1.创建配置对象
        final SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sparkCore");

        // 2. 创建sparkContext
        final JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("zhangsan", "lisi", "wangwu");

        // 操作内存数据
        JavaRDD<String> parallelize = sc.parallelize(list);

        List<String> collect = parallelize.collect();
        collect.forEach(System.out::println);

        sc.close();
    }
}
