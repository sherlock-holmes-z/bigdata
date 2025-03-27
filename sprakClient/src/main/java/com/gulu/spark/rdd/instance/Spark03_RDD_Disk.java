package com.gulu.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

/**
 * @author chocolate
 * 2025/3/27 15:03
 */
public class Spark03_RDD_Disk {
    public static void main(String[] args) {
        // 1.创建配置对象
        final SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("sparkCore");

        // 2. 创建sparkContext
        final JavaSparkContext sc = new JavaSparkContext(conf);

        // 操作磁盘文件
        JavaRDD<String> rdd = sc.textFile("src/main/resources/rdd/1.txt");
        List<String> collect = rdd.collect();
        collect.forEach(System.out::println);

        sc.close();
    }
}
